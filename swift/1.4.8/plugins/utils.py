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
from hashlib import md5
from ConfigParser import ConfigParser
from swift.common.utils import normalize_timestamp, TRUE_VALUES
from eventlet import sleep
import cPickle as pickle

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

def do_chown(path, uid, gid):
    try:
        os.chown(path, uid, gid)
    except Exception, err:
        logging.exception("Chown failed on %s err: %s", path, str(err))
        raise
    return True

def do_stat(path):
    try:
        #Check for fd.
        if isinstance(path, int):
            buf = os.fstat(path)
        else:
            buf = os.stat(path)
    except Exception, err:
        logging.exception("Stat failed on %s err: %s", path, str(err))
        raise

    return buf

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

def do_setxattr(path, key, value):
    fd = None
    if not os.path.isdir(path):
        fd = do_open(path, 'rb')
    else:
        fd = path
    if fd or os.path.isdir(path):
        try:
            xattr.set(fd, key, value)
        except Exception, err:
            logging.exception("xattr.set failed on %s key %s err: %s", path, key, str(err))
            raise
        finally:
            if fd and not os.path.isdir(path):
                do_close(fd)
    else:
        logging.error("Open failed path %s", path)
        return False
    return True

def do_getxattr(path, key, log = True):
    fd = None
    if not os.path.isdir(path):
        fd = do_open(path, 'rb')
    else:
        fd = path
    if fd or os.path.isdir(path):
        try:
            value = xattr.get(fd, key)
        except Exception, err:
            if log:
                logging.exception("xattr.get failed on %s key %s err: %s", path, key, str(err))
            raise
        finally:
            if fd and not os.path.isdir(path):
                do_close(fd)
    else:
        logging.error("Open failed path %s", path)
        return False
    return value

def do_removexattr(path, key):
    fd = None
    if not os.path.isdir(path):
        fd = do_open(path, 'rb')
    else:
        fd = path
    if fd or os.path.isdir(path):
        try:
            xattr.remove(fd, key)
        except Exception, err:
            logging.exception("xattr.remove failed on %s key %s err: %s", path, key, str(err))
            raise
        finally:
            if fd and not os.path.isdir(path):
                do_close(fd)
    else:
        logging.error("Open failed path %s", path)
        return False
    return True

def read_metadata(path):
    """
    Helper function to read the pickled metadata from a File/Directory .

    :param path: File/Directory to read metadata from.

    :returns: dictionary of metadata
    """
    metadata = ''
    key = 0
    while True:
        try:
            metadata += do_getxattr(path, '%s%s' % (METADATA_KEY, (key or '')),
                            log = False)
        except Exception:
            break
        key += 1
    if metadata:
        return pickle.loads(metadata)
    else:
        return {}


def write_metadata(path, metadata):
    """
    Helper function to write pickled metadata for a File/Directory.

    :param path: File/Directory path to write the metadata
    :param metadata: metadata to write
    """
    metastr = pickle.dumps(metadata, PICKLE_PROTOCOL)
    key = 0
    while metastr:
        do_setxattr(path, '%s%s' % (METADATA_KEY, key or ''), metastr[:254])
        metastr = metastr[254:]
        key += 1

def clean_metadata(path):
    key = 0
    while True:
        value = do_getxattr(path, '%s%s' % (METADATA_KEY, (key or '')))
        do_removexattr(path, '%s%s' % (METADATA_KEY, (key or '')))
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
    do_setxattr(path, 'user.test.key1', 'value1')
    try:
        xattr.remove(path, 'user.test.key1')
    except Exception, err:
        logging.exception("xattr.remove failed on %s err: %s", path, str(err))
        #Remove xattr may fail in case of concurrent remove.
    return True


def _check_valid_account(account, fs_object):
    mount_path = getattr(fs_object, 'mount_path', MOUNT_PATH)

    if os.path.ismount(os.path.join(mount_path, account)):
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
    etag = None
    if os.path.exists(path):
        etag = md5()
        if not os.path.isdir(path):
            fp = open(path, 'rb')
            if fp:
                while True:
                    chunk = fp.read(CHUNK_SIZE)
                    if chunk:
                        etag.update(chunk)
                    else:
                        break
                fp.close()

        etag = etag.hexdigest()

    return etag


def get_object_metadata(obj_path):
    """
    Return metadata of object.
    """
    metadata = {}
    if os.path.exists(obj_path):
        if not os.path.isdir(obj_path):
            metadata = {
                    X_TIMESTAMP: normalize_timestamp(os.path.getctime(obj_path)),
                    X_CONTENT_TYPE: FILE_TYPE,
                    X_ETAG: get_etag(obj_path),
                    X_CONTENT_LENGTH: os.path.getsize(obj_path),
                    X_TYPE: OBJECT,
                    X_OBJECT_TYPE: FILE,
                }
        else:
            metadata = {
                    X_TIMESTAMP: normalize_timestamp(os.path.getctime(obj_path)),
                    X_CONTENT_TYPE: DIR_TYPE,
                    X_ETAG: get_etag(obj_path),
                    X_CONTENT_LENGTH: 0,
                    X_TYPE: OBJECT,
                    X_OBJECT_TYPE: DIR,
                }

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

def restore_object(obj_path, metadata):
    meta = read_metadata(obj_path)
    if meta:
        meta.update(metadata)
        write_metadata(obj_path, meta)
    else:
        write_metadata(obj_path, metadata)

def restore_container(cont_path, metadata):
    meta = read_metadata(cont_path)
    if meta:
        meta.update(metadata)
        write_metadata(cont_path, meta)
    else:
        write_metadata(cont_path, metadata)

def restore_account(acc_path, metadata):
    meta = read_metadata(acc_path)
    if meta:
        meta.update(metadata)
        write_metadata(acc_path, meta)
    else:
        write_metadata(acc_path, metadata)

def create_object_metadata(obj_path):
    meta = get_object_metadata(obj_path)
    restore_object(obj_path, meta)
    return meta

def create_container_metadata(cont_path):
    meta = get_container_metadata(cont_path)
    restore_container(cont_path, meta)
    return meta

def create_account_metadata(acc_path):
    meta = get_account_metadata(acc_path)
    restore_account(acc_path, meta)
    return meta


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

