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

import os
import stat
import errno
from eventlet import tpool
from tempfile import mkstemp
from contextlib import contextmanager
from swift.common.exceptions import DiskFileNotExist
from swift.plugins.utils import mkdirs, rmdirs, validate_object, \
     create_object_metadata, do_open, do_ismount, do_close, do_unlink, \
     do_chown, do_stat, read_metadata, write_metadata, \
     get_object_metadata
from swift.plugins.utils import X_CONTENT_LENGTH, X_TIMESTAMP, \
     X_TYPE, X_OBJECT_TYPE, MARKER_DIR, DEFAULT_UID, DEFAULT_GID

import logging
from swift.obj.server import DiskFile


DATADIR = 'objects'
ASYNCDIR = 'async_pending'
KEEP_CACHE_SIZE = (5 * 1024 * 1024)
# keep these lower-case
DISALLOWED_HEADERS = set('content-length content-type deleted etag'.split())


class GlusterDiskFileError(Exception):
    pass


def make_directory(full_path, uid, gid, metadata=None):
    """
    Make a directory and change the owner ship as specified, and potentially
    creating the object metadata if requested.
    """
    try:
        # We know the parent directory exists, attempt the creation.
        os.mkdir(full_path)
    except OSError as err:
        if err.errno == errno.ENOENT:
            # Tell the caller some path to the parent directory does not
            # exist.
            return False
        elif err.errno == errno.EEXIST:
            # Possible race, in that the caller invoked this method when it
            # had previously determined the file did not exist.
            if not os.path.isdir(full_path):
                logging.exception("create_dir_object: os.mkdir failed"
                                  " on path %s because it already"
                                  " exists but not as a directory"
                                  " (err: %s)",
                                  full_path, err.strerror)
                # FIXME: Ideally we'd want to return an appropriate error
                # message and code in the PUT Object REST API response.
                raise GlusterDiskFileError()
            return True
        elif err.errno == errno.ENOTDIR:
            logging.exception("create_dir_object: os.mkdir failed"
                              " because some part of path %s is"
                              " not in fact a directory (err: %s)",
                              full_path, err.strerror)
            # FIXME: Ideally we'd want to return an appropriate error
            # message and code in the PUT Object REST API response.
            raise GlusterDiskFileError()
        else:
            # Some other potentially rare exception occurred that does not
            # currently warrant a special log entry to help diagnose.
            raise GlusterDiskFileError()
    else:
        if metadata:
            # We were asked to set the initial metadata for this object.
            metadata_orig = get_object_metadata(full_path)
            metadata_orig.update(metadata)
            write_metadata(full_path, metadata)

        # We created it, so we are reponsible for always setting the proper
        # ownership.
        do_chown(full_path, uid, gid)
        return True


class Gluster_DiskFile(DiskFile):
    """
    Manage object files on disk.

    :param path: path to devices on the node/mount path for UFO.
    :param device: device name/account_name for UFO.
    :param partition: partition on the device the object lives in
    :param account: account name for the object
    :param container: container name for the object
    :param obj: object name for the object
    :param keep_data_fp: if True, don't close the fp, otherwise close it
    :param disk_chunk_Size: size of chunks on file reads
    """

    def __init__(self, path, device, partition, account, container, obj,
                 logger, keep_data_fp=False, disk_chunk_size=65536,
                 uid=DEFAULT_UID, gid=DEFAULT_GID, fs_object=None):
        self.disk_chunk_size = disk_chunk_size
        device = fs_object.convert_account_to_device(account, unique=True)
        #Don't support obj_name ending/begining with '/', like /a, a/, /a/b/ etc
        obj = obj.strip('/')
        if '/' in obj:
            self.obj_path, self.obj = obj.rsplit('/', 1)
        else:
            self.obj_path = ''
            self.obj = obj

        if self.obj_path:
            self.name = os.path.join(container, self.obj_path)
        else:
            self.name = container
        self.device_path = os.path.join(path, device)
        #Absolute path for obj directory.
        self.datadir = os.path.join(self.device_path, self.name)

        if not do_ismount(self.device_path):
            fs_object.mount_via_account(path, account, unique=True)

        self.container_path = os.path.join(self.device_path, container)
        self.tmpdir = os.path.join(self.device_path, 'tmp')
        self.logger = logger
        self.metadata = {}
        self.meta_file = None
        self.data_file = None
        self.fp = None
        self.iter_etag = None
        self.started_at_0 = False
        self.read_to_eof = False
        self.quarantined_dir = None
        self.keep_cache = False
        self.is_dir = False
        self.is_valid = True
        self.uid = int(uid)
        self.gid = int(gid)
        data_file = os.path.join(self.datadir, self.obj)
        stats = do_stat(data_file)
        if not stats:
            return

        # File exists, find out what it is
        self.data_file = data_file
        self.is_dir = stat.S_ISDIR(stats.st_mode)

        self.metadata = read_metadata(data_file)
        if not self.metadata:
            create_object_metadata(data_file)
            self.metadata = read_metadata(data_file)

        if not validate_object(self.metadata):
            create_object_metadata(data_file)
            self.metadata = read_metadata(data_file)

        self.filter_metadata()

        if keep_data_fp:
            self.fp = do_open(self.data_file, 'rb')

    def close(self, verify_file=True):
        """
        Close the file. Will handle quarantining file if necessary.

        :param verify_file: Defaults to True. If false, will not check
                            file to see if it needs quarantining.
        """
        #Marker directory
        if self.is_dir:
            return
        if self.fp:
            do_close(self.fp)
            self.fp = None

    def is_deleted(self):
        """
        Check if the file is deleted.

        :returns: True if the file doesn't exist or has been flagged as
                  deleted.
        """
        return not self.data_file

    def create_dir_object(self, dir_path, metadata=None):
        """
        Create a directory object at the specified path. No check is made to
        see if the directory object already exists, that is left to the
        caller (this avoids a potentially duplicate stat() system call).

        The "dir_path" must be relative to its container, self.container_path.

        The "metadata" object is an optional set of metadata to apply to the
        newly created directory object. If not present, no initial metadata is
        applied.

        The algorithm used is as follows:

          1. An attempt is made to create the directory, assuming the parent
             directory already exists

             * Directory creation races are detected, returning success in
               those cases

          2. If the directory creation fails because some part of the path to
             the directory does not exist, then a search back up the path is
             performed to find the first existing ancestor directory, and then
             the missing parents are successively created, finally creating
             the target directory
        """
        full_path = os.path.join(self.container_path, dir_path)
        cur_path = full_path
        stack = []
        while True:
            md = None if cur_path != full_path else metadata
            if make_directory(cur_path, self.uid, self.gid, md):
                break
            # Some path of the parent did not exist, so loop around and
            # create that, pushing this parent on the stack.
            if os.path.sep not in cur_path:
                self.logger.error("Failed to create directory path while"
                                  " exhausting path elements to create: %s",
                                  full_path)
                raise GlusterDiskFileError()
            cur_path, child = cur_path.rsplit(os.path.sep, 1)
            assert child
            stack.append(child)

        child = stack.pop() if stack else None
        while child:
            cur_path = os.path.join(cur_path, child)
            md = None if cur_path != full_path else metadata
            if not make_directory(cur_path, self.uid, self.gid, md):
                self.logger.error("Failed to create directory path to"
                                  " target, %s, on subpath: %s",
                                  full_path, cur_path)
                raise GlusterDiskFileError()
            child = stack.pop() if stack else None
        return True

    def put_metadata(self, metadata):
        write_metadata(self.data_file, metadata)
        self.metadata = metadata

    def put(self, fd, tmppath, metadata, extension=''):
        """
        Finalize writing the file on disk, and renames it from the temp file
        to the real location.  This should be called after the data has been
        written to the temp file.

        :params fd: file descriptor of the temp file
        :param tmppath: path to the temporary file being used
        :param metadata: dictionary of metadata to be written
        :param extention: extension to be used when making the file
        """
        #Marker dir.
        if extension == '.ts':
            return
        if extension == '.meta':
            self.put_metadata(metadata)
            return

        if metadata[X_OBJECT_TYPE] == MARKER_DIR:
            if not self.data_file:
                # Does not exist, create it
                data_file = os.path.join(self.obj_path, self.obj)
                self.create_dir_object(data_file, metadata)
                self.data_file = os.path.join(self.container_path, data_file)
            elif not self.is_dir:
                # Exists, but as a file
                self.logger.error('Directory creation failed since a file'
                                  ' already exists: %s' % self.data_file)
                raise GlusterDiskFileError()
            return

        # Check if directory already exists and we are trying to put a regular
        # file
        if self.is_dir:
            self.logger.error('File creation failed since directory already'
                              ' exists %s' % self.data_file)
            raise GlusterDiskFileError()

        if X_CONTENT_LENGTH in metadata:
            self.drop_cache(fd, 0, int(metadata[X_CONTENT_LENGTH]))

        tpool.execute(os.fsync, fd)

        # FIXME: Should use fd instead of path to avoid extra lookup
        write_metadata(tmppath, metadata)

        # Ensure it is properly owned before we make it available.
        do_chown(fd, self.uid, self.gid)

        if not self.data_file and self.obj_path \
                and not os.path.exists(os.path.join(self.container_path, self.obj_path)):
            self.create_dir_object(self.obj_path)

        # At this point we know that the object's full directory path exists,
        # so we can just rename it directly without using Swift's
        # swift.common.utils.renamer(), which makes the directory path and
        # adds extra stat() calls.
        data_file = os.path.join(self.datadir, self.obj)
        os.rename(tmppath, data_file)

        # Avoid the unlink() system call as part of the mkstemp context cleanup
        self.tmppath = None

        self.metadata = metadata

        # Mark that is actually exists
        self.data_file = data_file

    def unlinkold(self, timestamp):
        """
        Remove any older versions of the object file.  Any file that has an
        older timestamp than timestamp will be deleted.

        :param timestamp: timestamp to compare with each file
        """
        if self.metadata and self.metadata[X_TIMESTAMP] != timestamp:
            self.unlink()

    def unlink(self):
        """
        Remove the file.
        """
        #Marker dir.
        if self.is_dir:
            rmdirs(os.path.join(self.datadir, self.obj))
            if not os.path.isdir(os.path.join(self.datadir, self.obj)):
                self.metadata = {}
                self.data_file = None
            else:
                logging.error('Unable to delete dir %s' % os.path.join(self.datadir, self.obj))
            return

        assert self.data_file == os.path.join(self.datadir, self.obj)
        do_unlink(self.data_file)

        self.metadata = {}
        self.data_file = None

    def get_data_file_size(self):
        """
        Returns the os.path.getsize for the file.  Raises an exception if this
        file does not match the Content-Length stored in the metadata. Or if
        self.data_file does not exist.

        :returns: file size as an int
        :raises DiskFileError: on file size mismatch.
        :raises DiskFileNotExist: on file not existing (including deleted)
        """
        #Marker directory.
        if self.is_dir:
            return 0
        try:
            file_size = 0
            if self.data_file:
                file_size = os.path.getsize(self.data_file)
                if  X_CONTENT_LENGTH in self.metadata:
                    metadata_size = int(self.metadata[X_CONTENT_LENGTH])
                    if file_size != metadata_size:
                        self.metadata[X_CONTENT_LENGTH] = file_size
                        self.update_object(self.metadata)

                return file_size
        except OSError, err:
            if err.errno != errno.ENOENT:
                raise
        raise DiskFileNotExist('Data File does not exist.')

    def update_object(self, metadata):
        write_metadata(self.data_file, metadata)
        self.metadata = metadata

    def filter_metadata(self):
        if X_TYPE in self.metadata:
            self.metadata.pop(X_TYPE)
        if X_OBJECT_TYPE in self.metadata:
            self.metadata.pop(X_OBJECT_TYPE)

    @contextmanager
    def mkstemp(self):
        """Contextmanager to make a temporary file."""

        if not os.path.exists(self.tmpdir):
            mkdirs(self.tmpdir)
        fd, tmppath = mkstemp(dir=self.tmpdir)
        self.tmppath = tmppath
        try:
            yield fd, tmppath
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
            try:
                if self.tmppath:
                    os.unlink(self.tmppath)
            except OSError:
                self.tmppath = None
