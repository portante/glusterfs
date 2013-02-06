# Copyright (c) 2011 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import os, fcntl, time, errno
from ConfigParser import ConfigParser, NoSectionError, NoOptionError
from swift.common.utils import TRUE_VALUES
from swift.plugins.utils import mkdirs, do_ismount

_mount_path = '/mnt/gluster-object'
_mount_ip = 'localhost'
_remote_cluster = False
_object_only = False
_allow_mount_per_server = False

_fs_conf = ConfigParser()
if _fs_conf.read(os.path.join('/etc/swift', 'fs.conf')):
    try:
        _mount_path = _fs_conf.get('DEFAULT', 'mount_path', _mount_path)
    except (NoSectionError, NoOptionError):
        pass
    try:
        _mount_ip = _fs_conf.get('DEFAULT', 'mount_ip', _mount_ip)
    except (NoSectionError, NoOptionError):
        pass
    try:
        _remote_cluster = _fs_conf.get('DEFAULT', 'remote_cluster', _remote_cluster) in TRUE_VALUES
    except (NoSectionError, NoOptionError):
        pass
    try:
        _object_only = _fs_conf.get('DEFAULT', 'object_only', _object_only) in TRUE_VALUES
    except (NoSectionError, NoOptionError):
        pass
    try:
        _allow_mount_per_server = _fs_conf.get('DEFAULT', 'allow_mount_per_server', _allow_mount_per_server) in TRUE_VALUES
    except (NoSectionError, NoOptionError):
        pass

def _get_unique_id():
    # Each individual server will attempt to get a free lock file
    # sequentially numbered, storing the pid of the holder of that
    # file, That number represents the numbered mount point to use
    # for its operations.
    if not _allow_mount_per_server:
        return 0
    lock_dir  = "/var/lib/gluster/swift/run/"
    if not os.path.exists(lock_dir):
        mkdirs(lock_dir)
    unique_id = 0
    lock_file_template = os.path.join(lock_dir, 'swift.object-server-%03d.lock')
    for i in range(1, 201):
        lock_file = lock_file_template % i
        fd = os.open(lock_file, os.O_CREAT|os.O_RDWR)
        try:
            fcntl.lockf(fd, fcntl.LOCK_EX|fcntl.LOCK_NB)
        except IOError as ex:
            os.close(fd)
            if ex.errno in (errno.EACCES, errno.EAGAIN):
                # This means that some other process has it locked, so they
                # own the lock.
                continue
            raise
        except:
            os.close(fd)
            raise
        else:
            # We got the lock, write our PID into it, but don't close the
            # file, it will be closed when our process exists
            os.lseek(fd, 0, os.SEEK_SET)
            pid = str(os.getpid()) + '\n'
            os.write(fd, pid)
            unique_id = i
            break
    return unique_id

_unique_id = None


class GlusterfsFailureToMountError(Exception):
    pass


class GlusterfsHostCommunicationError(Exception):
    pass


class GlusterfsAccountNotMappedError(Exception):
    pass


class Glusterfs(object):
    def __init__(self):
        self.name = 'glusterfs'
        self.mount_path = _mount_path
        self.mount_ip = _mount_ip
        self.remote_cluster = _remote_cluster
        self.object_only = _object_only

    def convert_account_to_device(self, account, unique=False):
        """
        Convert a Swift account name to a gluster mount point name.

        If unique is False, then we just map the account directly to the
        device name. If unique is True, then we determine a unique mount point
        name that maps to our PID.
        """
        if not unique:
            # One-to-one mapping of account to device mount point name
            device = account
        else:
            global _unique_id
            if _unique_id is None:
                _unique_id = _get_unique_id()
            device = ("%s_%03d" % (account, _unique_id)) if _unique_id else account
        return device

    def mount_via_account(self, path, account, unique=False):
        if path != self.mount_path:
            logging.warn('Unexpected mount path: %s, expected: %s' % (
                path, self.mount_path))
            path = self.mount_path

        device = self.convert_account_to_device(account, unique)

        final_path = os.path.join(path, device)
        if do_ismount(final_path):
            return

        export = self._get_export_from_account_id(account)
        if not export:
            raise GlusterfsAccountNotMappedError(
                'Account, %s, does not map to an exported gluster volume name'\
                % account)

        if not os.path.isdir(final_path):
            mkdirs(final_path)

        if not self._mount(device, export):
            raise GlusterfsFailureToMountError('Failed to mount: %s',
                                               final_path)

    def _busy_wait(self, mount_path):
        # Iterate for definite number of time over a given
        # interval for successful mount
        for i in range(0, 5):
            if do_ismount(mount_path):
                return True
            time.sleep(2)
        logging.error('Mount failed (after timeout) %s: %s' % (
            self.name, mount_path))
        return False

    def _mount(self, device, export):
        final_path = os.path.join(self.mount_path, device)

        mnt_cmd = 'mount -t glusterfs %s:%s %s' % (self.mount_ip, export, \
                                                   final_path)
        if _allow_mount_per_server:
            if os.system(mnt_cmd):
                raise GlusterfsFailureToMountError('Mount failed %s: %s' % (
                    self.name, mnt_cmd))
            return True

        lock_dir  = "/var/lib/glusterd/vols/%s/run/" % export
        if not os.path.exists(lock_dir):
            mkdirs(lock_dir)
        lock_file = os.path.join(lock_dir, 'swift.%s.lock' % device);

        fd = os.open(lock_file, os.O_CREAT|os.O_RDWR)
        with os.fdopen(fd, 'r+b') as f:
            try:
                fcntl.lockf(f, fcntl.LOCK_EX|fcntl.LOCK_NB)
            except IOError as ex:
                if ex.errno in (errno.EACCES, errno.EAGAIN):
                    # This means that some other process is mounting the
                    # filesystem, so wait for the mount process to complete
                    return self._busy_wait(final_path)

            if os.system(mnt_cmd) or not self._busy_wait(final_path):
                logging.error('Mount failed %s: %s' % (self.name, mnt_cmd))
                return False
        return True

    def _get_export_list_local(self):
        export_list = []
        cmnd = 'gluster volume info'

        if os.system(cmnd + ' >> /dev/null'):
            raise GlusterfsHostCommunicationError('Getting volume failed %s',
                                                  self.name)

        fp = os.popen(cmnd)
        while True:
            item = fp.readline()
            if not item:
                break
            item = item.strip('\n').strip(' ')
            if item.lower().startswith('volume name:'):
                export_list.append(item.split(':')[1].strip(' '))

        return export_list

    def _get_export_list_remote(self):
        export_list = []
        cmnd = 'ssh %s gluster volume info' % self.mount_ip

        if os.system(cmnd + ' >> /dev/null'):
            raise GlusterfsHostCommunicationError('Getting volume info failed'
                                                  ' %s, make sure to have'
                                                  ' passwordless ssh on %s',
                                                  self.name, self.mount_ip)
        fp = os.popen(cmnd)
        while True:
            item = fp.readline()
            if not item:
                break
            item = item.strip('\n').strip(' ')
            if item.lower().startswith('volume name:'):
                export_list.append(item.split(':')[1].strip(' '))

        return export_list

    def _get_export_list(self):
        if self.remote_cluster:
            return self._get_export_list_remote()
        else:
            return self._get_export_list_local()

    def _get_export_from_account_id(self, account):
        if not account:
            raise ValueError("Account is: %r" % account)
        if not account.startswith('AUTH_'):
            raise ValueError("Invalid account name, %s, "
                             "does not start with AUTH_" % account)

        export_list = self._get_export_list()
        for export in export_list:
            if account == 'AUTH_' + export:
                return export

        logging.error('No export found matching "%s" in %r', account, export_list)
        return None
