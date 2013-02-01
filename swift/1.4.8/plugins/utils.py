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
import os
import errno
import xattr
import stat
from hashlib import md5
from swift.common.utils import normalize_timestamp, TRUE_VALUES
from eventlet import sleep
import cPickle as pickle
from ConfigParser import ConfigParser, NoSectionError, NoOptionError

X_CONTENT_TYPE = 'Content-Type'
X_CONTENT_LENGTH = 'Content-Length'
X_TIMESTAMP = 'X-Timestamp'
X_PUT_TIMESTAMP = 'X-PUT-Timestamp'
X_TYPE = 'X-Type'
X_ETAG = 'ETag'
X_OBJECTS_COUNT = 'X-Object-Count'
X_BYTES_USED = 'X-Bytes-Used'
X_CONTAINER_COUNT = 'X-Container-Count'
X_OBJECT_TYPE = 'X-Object-Type'
DIR_TYPE = 'application/directory'
ACCOUNT = 'Account'
MOUNT_PATH = '/mnt/gluster-object'
METADATA_KEY = 'user.swift.metadata'
MAX_XATTR_SIZE = 254
CONTAINER = 'container'
DIR = 'dir'
MARKER_DIR = 'marker_dir'
FILE = 'file'
DIR_TYPE = 'application/directory'
FILE_TYPE = 'application/octet-stream'
OBJECT = 'Object'
OBJECT_TYPE = 'application/octet-stream'
DEFAULT_UID = -1
DEFAULT_GID = -1
PICKLE_PROTOCOL = 2
CHUNK_SIZE = 65536

_fs_conf = ConfigParser()
_fs_conf.read(os.path.join('/etc/swift', 'fs.conf'))
_do_getsize = _fs_conf.get('DEFAULT', 'accurate_size_in_listing', "no") in TRUE_VALUES

def mkdirs(path):
    """
    Ensures the path is a directory or makes it if not. Errors if the path
    exists but is a file or on permissions failure.

    :param path: path to create
    """
    if not os.path.isdir(path):
        try:
            do_makedirs(path)
        except OSError, err:
            #TODO: check, isdir will fail if mounted and volume stopped.
            #if err.errno != errno.EEXIST or not os.path.isdir(path)
            if err.errno != errno.EEXIST:
                raise

def rmdirs(path):
    if os.path.isdir(path) and dir_empty(path):
        do_rmdir(path)
    else:
        logging.error("rmdirs failed dir may not be empty or not valid dir")
        return False

def strip_obj_storage_path(path, string='/mnt/gluster-object'):
    """
    strip /mnt/gluster-object
    """
    return path.replace(string, '').strip('/')

def do_ismount(path):
    """
    Test whether a path is a mount point.

    This is code hijacked from C Python 2.6.8, adapted to remove the extra
    lstat() system call.
    """
    try:
        s1 = os.lstat(path)
    except os.error:
        # It doesn't exist -- so not a mount point :-)
        return False

    if stat.S_ISLNK(s1.st_mode):
        # A symlink can never be a mount point
        return False

    try:
        s2 = os.lstat(os.path.join(path, '..'))
    except os.error:
        # It doesn't exist -- so not a mount point :-)
        return False

    dev1 = s1.st_dev
    dev2 = s2.st_dev
    if dev1 != dev2:
        # path/.. on a different device as path
        return True

    ino1 = s1.st_ino
    ino2 = s2.st_ino
    if ino1 == ino2:
        # path/.. is the same i-node as path
        return True

    return False

def do_mkdir(path):
    try:
        os.mkdir(path)
    except Exception, err:
        logging.exception("Mkdir failed on %s err: %s", path, str(err))
        if err.errno != errno.EEXIST:
            raise
    return True

def do_makedirs(path):
    try:
        os.makedirs(path)
    except Exception, err:
        logging.exception("Makedirs failed on %s err: %s", path, str(err))
        if err.errno != errno.EEXIST:
            raise
    return True


def do_listdir(path):
    try:
        buf = os.listdir(path)
    except Exception, err:
        logging.exception("Listdir failed on %s err: %s", path, str(err))
        raise
    return buf

def do_chown(path_or_fd, uid, gid):
    try:
        if isinstance(path_or_fd, int):
            os.fchown(path_or_fd, uid, gid)
        else:
            os.chown(path_or_fd, uid, gid)
    except Exception, err:
        logging.exception("*chown() failed on %s err: %s", path_or_fd, str(err))
        raise
    return True

def do_stat(path):
    try:
        stats = os.stat(path)
    except OSError as err:
        if err.errno != errno.ENOENT:
            logging.exception("Stat failed on %s err: %s", path, str(err))
            raise
        stats = None
    return stats

def do_open(path, mode):
    try:
        fd = open(path, mode)
    except Exception, err:
        logging.exception("Open failed on %s err: %s", path, str(err))
        raise
    return fd

def do_close(fd):
    #fd could be file or int type.
    try:
        if isinstance(fd, int):
            os.close(fd)
        else:
            fd.close()
    except Exception, err:
        logging.exception("Close failed on %s err: %s", fd, str(err))
        raise
    return True

def do_unlink(path, log = True):
    try:
        os.unlink(path)
    except Exception, err:
        if log:
            logging.exception("Unlink failed on %s err: %s", path, str(err))
        if err.errno != errno.ENOENT:
            raise
    return True

def do_rmdir(path):
    try:
        os.rmdir(path)
    except Exception, err:
        logging.exception("Rmdir failed on %s err: %s", path, str(err))
        if err.errno != errno.ENOENT:
            raise
    return True

def do_rename(old_path, new_path):
    try:
        os.rename(old_path, new_path)
    except Exception, err:
        logging.exception("Rename failed on %s to %s  err: %s", old_path, new_path, \
                          str(err))
        raise
    return True

def read_metadata(path):
    """
    Helper function to read the pickled metadata from a File/Directory.

    :param path: File/Directory to read metadata from.

    :returns: dictionary of metadata
    """
    metadata = None
    metadata_s = ''
    key = 0
    while metadata is None:
        try:
            metadata_s += xattr.get(path, '%s%s' % (METADATA_KEY, (key or '')))
        except IOError as err:
            if err.errno == errno.ENODATA:
                if key > 0:
                    # No errors reading the xattr keys, but since we have not
                    # been able to find enough chunks to get a successful
                    # unpickle operation, we consider the metadata lost, and
                    # drop the existing data so that the internal state can be
                    # recreated.
                    clean_metadata(path)
                # We either could not find any metadata key, or we could find
                # some keys, but were not successful in performing the
                # unpickling (missing keys perhaps)? Either way, just report
                # to the caller we have no metadata.
                metadata = {}
            else:
                logging.exception("xattr.get failed on %s key %s err: %s",
                                  path, key, str(err))
                # Note that we don't touch the keys on errors fetching the
                # data since it could be a transient state.
                raise
        else:
            try:
                # If this key provides all or the remaining part of the pickle
                # data, we don't need to keep searching for more keys. This
                # means if we only need to store data in N xattr key/value
                # pair, we only need to invoke xattr get N times. With large
                # keys sizes we are shooting for N = 1.
                metadata = pickle.loads(metadata_s)
                assert isinstance(metadata, dict)
            except EOFError, pickle.UnpicklingError:
                # We still are not able recognize this existing data collected
                # as a pickled object. Make sure we loop around to try to get
                # more from another xattr key.
                metadata = None
                key += 1
    return metadata

def write_metadata(path, metadata):
    """
    Helper function to write pickled metadata for a File/Directory.

    :param path: File/Directory path to write the metadata
    :param metadata: dictionary of metadata write
    """
    assert isinstance(metadata, dict)
    metastr = pickle.dumps(metadata, PICKLE_PROTOCOL)
    key = 0
    while metastr:
        try:
            xattr.set(path, '%s%s' % (METADATA_KEY, key or ''), metastr[:MAX_XATTR_SIZE])
        except IOError as err:
            logging.exception("xattr.set failed on %s key %s err: %s", path, key, str(err))
            raise
        metastr = metastr[MAX_XATTR_SIZE:]
        key += 1

def clean_metadata(path):
    key = 0
    while True:
        try:
            xattr.remove(path, '%s%s' % (METADATA_KEY, (key or '')))
        except IOError as err:
            if err.errno == errno.ENODATA:
                break
            raise
        key += 1

def dir_empty(path):
    """
    Return true if directory/container is empty.
    :param path: Directory path.
    :returns: True/False.
    """
    if os.path.isdir(path):
        try:
            files = do_listdir(path)
        except Exception, err:
            logging.exception("listdir failed on %s err: %s", path, str(err))
            raise
        if not files:
            return True
        else:
            return False
    else:
        if not os.path.exists(path):
            return True

def get_device_from_account(account):
    if account.startswith(RESELLER_PREFIX):
        device = account.replace(RESELLER_PREFIX, '', 1)
        return device

def check_user_xattr(path):
    if not os.path.exists(path):
        return False
    try:
        xattr.set(path, 'user.test.key1', 'value1')
    except IOError as err:
        logging.exception("check_user_xattr: set failed on %s err: %s", path, str(err))
        raise
    try:
        xattr.remove(path, 'user.test.key1')
    except IOError as err:
        logging.exception("check_user_xattr: remove failed on %s err: %s", path, str(err))
        #Remove xattr may fail in case of concurrent remove.
    return True

def _check_valid_account(account, fs_object):
    mount_path = getattr(fs_object, 'mount_path', MOUNT_PATH)

    if do_ismount(os.path.join(mount_path, account)):
        return True

    if not check_account_exists(fs_object.get_export_from_account_id(account), fs_object):
        logging.error('Account not present %s', account)
        return False

    if not os.path.isdir(os.path.join(mount_path, account)):
        mkdirs(os.path.join(mount_path, account))

    if fs_object:
        if not fs_object.mount(account):
            return False

    return True

def check_valid_account(account, fs_object):
    return _check_valid_account(account, fs_object)

def validate_container(metadata):
    if not metadata:
        logging.error('No metadata')
        return False

    if X_TYPE not in metadata.keys() or \
       X_TIMESTAMP not in metadata.keys() or \
       X_PUT_TIMESTAMP not in metadata.keys() or \
       X_OBJECTS_COUNT not in metadata.keys() or \
       X_BYTES_USED not in metadata.keys():
        #logging.error('Container error %s' % metadata)
        return False

    if metadata[X_TYPE] == CONTAINER:
        return True

    logging.error('Container error %s' % metadata)
    return False

def validate_account(metadata):
    if not metadata:
        logging.error('No metadata')
        return False

    if X_TYPE not in metadata.keys() or \
       X_TIMESTAMP not in metadata.keys() or \
       X_PUT_TIMESTAMP not in metadata.keys() or \
       X_OBJECTS_COUNT not in metadata.keys() or \
       X_BYTES_USED not in metadata.keys() or \
       X_CONTAINER_COUNT not in metadata.keys():
        #logging.error('Account error %s' % metadata)
        return False

    if metadata[X_TYPE] == ACCOUNT:
        return True

    logging.error('Account error %s' % metadata)
    return False

def validate_object(metadata):
    if not metadata:
        logging.error('No metadata')
        return False

    if X_TIMESTAMP not in metadata.keys() or \
       X_CONTENT_TYPE not in metadata.keys() or \
       X_ETAG not in metadata.keys() or \
       X_CONTENT_LENGTH not in metadata.keys() or \
       X_TYPE not in metadata.keys() or \
       X_OBJECT_TYPE not in metadata.keys():
        #logging.error('Object error %s' % metadata)
        return False

    if metadata[X_TYPE] == OBJECT:
        return True

    logging.error('Object error %s' % metadata)
    return False

def is_marker(metadata):
    if not metadata:
        logging.error('No metadata')
        return False

    if X_OBJECT_TYPE not in metadata.keys():
        logging.error('X_OBJECT_TYPE missing %s' % metadata)
        return False

    if metadata[X_OBJECT_TYPE] == MARKER_DIR:
        return True
    else:
        return False

def _update_list(path, cont_path, src_list, reg_file=True, object_count=0,
                 bytes_used=0, obj_list=[]):
    obj_path = strip_obj_storage_path(path, cont_path)

    for i in src_list:
        if obj_path:
            obj_list.append(os.path.join(obj_path, i))
        else:
            obj_list.append(i)

        object_count += 1

        if reg_file and _do_getsize:
            bytes_used += os.path.getsize(path + '/' + i)
            sleep()

    return object_count, bytes_used

def update_list(path, cont_path, dirs=[], files=[], object_count=0,
                bytes_used=0, obj_list=[]):
    if files:
        object_count, bytes_used = _update_list (path, cont_path, files, True,
                                                 object_count, bytes_used,
                                                 obj_list)
    if dirs:
        object_count, bytes_used = _update_list (path, cont_path, dirs, False,
                                                 object_count, bytes_used,
                                                 obj_list)
    return object_count, bytes_used

def get_container_details(cont_path):
    """
    get container details by traversing the filesystem
    """
    bytes_used = 0
    object_count = 0
    obj_list=[]

    if os.path.isdir(cont_path):
        for (path, dirs, files) in os.walk(cont_path):
            object_count, bytes_used = update_list(path, cont_path, dirs, files,
                                                   object_count, bytes_used,
                                                   obj_list)
            sleep()

    return obj_list, object_count, bytes_used

def get_account_details(acc_path):
    """
    Return container_list and container_count.
    """
    container_list = []
    container_count = 0

    if os.path.isdir(acc_path):
        for name in do_listdir(acc_path):
            if not os.path.isdir(acc_path + '/' + name) or \
               name.lower() == 'tmp' or name.lower() == 'async_pending':
                continue
            container_count += 1
            container_list.append(name)

    return container_list, container_count

def get_etag(path):
    etag = md5()
    with open(path, 'rb') as fp:
        while True:
            chunk = fp.read(CHUNK_SIZE)
            if chunk:
                etag.update(chunk)
            else:
                break
    return etag.hexdigest()

def get_object_metadata(obj_path):
    """
    Return metadata of object.
    """
    stats = do_stat(obj_path)
    if stats:
        is_dir = stat.S_ISDIR(stats.st_mode)
        metadata = {
            X_TYPE: OBJECT,
            X_TIMESTAMP: normalize_timestamp(stats.st_ctime),
            X_CONTENT_TYPE: DIR_TYPE if is_dir else FILE_TYPE,
            X_OBJECT_TYPE: DIR if is_dir else FILE,
            X_CONTENT_LENGTH: 0 if is_dir else stats.st_size,
            X_ETAG: md5().hexdigest() if is_dir else get_etag(obj_path),
            }
    else:
        metadata = {}
    return metadata

def get_container_metadata(cont_path):
    objects = []
    object_count = 0
    bytes_used = 0
    objects, object_count, bytes_used = get_container_details(cont_path)
    metadata = {X_TYPE: CONTAINER,
                X_TIMESTAMP: normalize_timestamp(os.path.getctime(cont_path)),
                X_PUT_TIMESTAMP: normalize_timestamp(os.path.getmtime(cont_path)),
                X_OBJECTS_COUNT: object_count,
                X_BYTES_USED: bytes_used}
    return metadata

def get_account_metadata(acc_path):
    containers = []
    container_count = 0
    containers, container_count = get_account_details(acc_path)
    metadata = {X_TYPE: ACCOUNT,
                X_TIMESTAMP: normalize_timestamp(os.path.getctime(acc_path)),
                X_PUT_TIMESTAMP: normalize_timestamp(os.path.getmtime(acc_path)),
                X_OBJECTS_COUNT: 0,
                X_BYTES_USED: 0,
                X_CONTAINER_COUNT: container_count}
    return metadata

def restore_metadata(path, metadata):
    meta_orig = read_metadata(path)
    if meta_orig:
        meta_new = meta_orig.copy()
        meta_new.update(metadata)
    else:
        meta_new = metadata
    if meta_orig != meta_new:
        write_metadata(path, meta_new)
    return meta_new

def create_object_metadata(obj_path):
    metadata = get_object_metadata(obj_path)
    return restore_metadata(obj_path, metadata)

def create_container_metadata(cont_path):
    metadata = get_container_metadata(cont_path)
    return restore_metadata(cont_path, metadata)

def create_account_metadata(acc_path):
    metadata = get_account_metadata(acc_path)
    return restore_metadata(acc_path, metadata)

def check_account_exists(account, fs_object):
    if account not in get_account_list(fs_object):
        logging.error('Account not exists %s' % account)
        return False
    else:
        return True

def get_account_list(fs_object):
    account_list = []
    if fs_object:
        account_list = fs_object.get_export_list()
    return account_list

def get_account_id(account):
    return RESELLER_PREFIX + md5(account + HASH_PATH_SUFFIX).hexdigest()
