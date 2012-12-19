# Copyright (c) 2012 Red Hat, Inc.
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

import os, errno

from gluster.swift.common.fs_utils import dir_empty, rmdirs, do_mkdir
from gluster.swift.common.utils import validate_account, validate_container, \
     get_container_details, get_account_details, create_container_metadata, \
     create_account_metadata, DEFAULT_GID, DEFAULT_UID, validate_object, \
     create_object_metadata, read_metadata, write_metadata, X_CONTENT_TYPE, \
     X_CONTENT_LENGTH, X_TIMESTAMP, X_PUT_TIMESTAMP, X_ETAG, X_OBJECTS_COUNT, \
     X_BYTES_USED, X_CONTAINER_COUNT
from gluster.swift.common import Glusterfs

from swift.common.utils import TRUE_VALUES


DATADIR = 'containers'

# Create a dummy db_file in /etc/swift
_unittests_enabled = os.getenv('GLUSTER_UNIT_TEST_ENABLED', 'no')
if _unittests_enabled in TRUE_VALUES:
    _tmp_dir = '/tmp/gluster_unit_tests'
    try:
        os.mkdir(_tmp_dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    _db_file = os.path.join(_tmp_dir, 'db_file.db')
else:
    _db_file = '/etc/swift/db_file.db'
if not os.path.exists(_db_file):
    file(_db_file, 'w+')


def _read_metadata(dd):
    """ Filter read metadata so that it always returns a tuple that includes
        some kind of timestamp. With 1.4.8 of the Swift integration the
        timestamps were not stored. Here we fabricate timestamps for volumes
        where the existing data has no timestamp (that is, stored data is not
        a tuple), allowing us a measure of backward compatibility.

        FIXME: At this time it does not appear that the timestamps on each
        metadata are used for much, so this should not hurt anything.
    """
    metadata_i = read_metadata(dd)
    metadata = {}
    timestamp = 0
    for key, value in metadata_i.iteritems():
        if not isinstance(value, tuple):
            value = (value, timestamp)
        metadata[key] = value
    return metadata

def filter_end_marker(objects, end_marker):
    """
    Accept a list of strings, sorted, and return all the strings that are
    strictly less than the given end_marker string. We perform this as a
    generator to avoid creating potentially large intermediate object lists.
    """
    for object_name in objects:
        if object_name < end_marker:
            yield object_name
        else:
            break

def filter_marker(objects, marker):
    """
    Accept sorted list of strings, return all strings whose value is strictly
    greater than the given marker value.
    """
    for object_name in objects:
        if object_name > marker:
            yield object_name

def filter_prefix(objects, prefix):
    """
    Accept a sorted list of strings, returning all strings starting with the
    given prefix.
    """
    found = False
    for object_name in objects:
        if object_name.startswith(prefix):
            yield object_name
            found = True
        else:
            # Since the list is assumed to be sorted, once we find an object
            # name that does not start with the prefix we know we won't find
            # any others, so we exit early.
            if found:
                break

def filter_delimiter(objects, delimiter, prefix):
    """
    Accept a sorted list of strings and return strings will only
    be those that start with the prefix.
    """
    assert delimiter and len(delimiter) == 1 and ord(delimiter) <= 254
    last_obj_name = ''
    if prefix:
        for object_name in objects:
            tmp_obj = object_name.replace(prefix, '', 1)
            suffix = tmp_obj.split(delimiter, 1)
            new_obj_name = prefix + suffix[0]
            if new_obj_name and new_obj_name != last_obj_name:
                last_obj_name = new_obj_name
                yield new_obj_name
    else:
        for object_name in objects:
            new_obj_name = object_name.split(delimiter, 1)[0]
            if not new_obj_name:
                continue
            new_obj_name += delimiter
            if new_obj_name != last_obj_name:
                last_obj_name = new_obj_name
                yield new_obj_name

def filter_limit(objects, limit):
    """
    Accept a list or a generator of strings and return a list object
    containing at most "limit" number of strings.
    """
    assert limit > 0
    cnt = 0
    for object_name in objects:
        yield object_name
        cnt += 1
        if cnt >= limit:
            break


class DiskCommon(object):
    """
    Common fields and methods shared between DiskDir and DiskAccount classes.
    """
    def __init__(self, root, drive, account, logger):
        # WARNING: The following four fields are referenced as fields by our
        # callers outside of this module, do not remove.
        self.metadata = {}
        self.db_file = _db_file
        self.pending_timeout = 0
        self.stale_reads_ok = False
        # The following fields are common
        self.root = root
        assert logger is not None
        self.logger = logger
        self.account = account
        self.datadir = os.path.join(root, drive)
        self._dir_exists = None

    def _dir_exists_read_metadata(self):
        self._dir_exists = os.path.exists(self.datadir)
        if self._dir_exists:
            self.metadata = _read_metadata(self.datadir)
        return self._dir_exists

    def initialize(self, timestamp):
        # This is always a NOOP since we always have a db_file created, and
        # method is only invoked by our caller when a db_file does NOT
        # exist. It should never actually get called.
        assert os.path.exists(self.db_file)

    def is_deleted(self):
        # The intention of this method is to check the file system to see if
        # the directory actually exists.
        return not os.path.exists(self.datadir)

    def update_metadata(self, metadata):
        assert self.metadata, "Valid container/account metadata should have " \
                "been created by now"
        if metadata:
            new_metadata = self.metadata.copy()
            new_metadata.update(metadata)
            if new_metadata != self.metadata:
                write_metadata(self.datadir, new_metadata)
                self.metadata = new_metadata


class DiskDir(DiskCommon):
    """
    Manage object files on disk.

    :param path: path to devices on the node
    :param drive: gluster volume drive name
    :param account: account name for the object
    :param container: container name for the object
    :param logger: account or container server logging object
    :param uid: user ID container object should assume
    :param gid: group ID container object should assume

    Usage pattern from container/server.py:
        DELETE:
            .db_file
            .initialize()
            if obj:
                .delete_object()
            else:
                .emtpy()
                .get_info()
                .is_deleted()
                .delete_db()
                .is_deleted()
                account_update():
                    .get_info()
        PUT:
            if obj:
                .db_file
                .initialize()
                .put_object()
            else:
                .db_file
                .initialize()
                .is_deleted()
                .update_put_timestamp()
                .is_deleted()
                .metadata
                .set_x_container_sync_points()
                .update_metadata()
                account_update():
                    .get_info()
        HEAD:
            .pending_timeout
            .stale_reads_ok
            .is_deleted()
            .get_info()
            .metadata
        GET:
            .pending_timeout
            .stale_reads_ok
            .is_deleted()
            .get_info()
            .metadata
            .list_objects_iter()
        POST:
            .is_deleted()
            .metadata
            .set_x_container_sync_points()
            .update_metadata()
    """

    def __init__(self, path, drive, account, container, logger,
                 uid=DEFAULT_UID, gid=DEFAULT_GID):
        super(DiskDir, self).__init__(path, drive, account, logger)

        self._object_info = None
        self.uid = int(uid)
        self.gid = int(gid)

        self.container = container
        self.datadir = os.path.join(self.datadir, self.container)

        if not self._dir_exists_read_metadata():
            return

        if not self.metadata:
            create_container_metadata(self.datadir)
            self.metadata = _read_metadata(self.datadir)
        else:
            if not validate_container(self.metadata):
                create_container_metadata(self.datadir)
                self.metadata = _read_metadata(self.datadir)

    def empty(self):
        return dir_empty(self.datadir)

    def list_objects_iter(self, limit, marker, end_marker,
                          prefix, delimiter, path=None, format=None):
        """
        Returns tuple of name, created_at, size, content_type, etag.
        """
        assert limit >= 0
        assert not delimiter or (len(delimiter) == 1 and ord(delimiter) <= 254)
        if path is not None:
            if path:
                prefix = path = path.rstrip('/') + '/'
            else:
                prefix = path
            delimiter = '/'
        elif delimiter and not prefix:
            # Ensure no prefix is represented as the empty string if a
            # delimiter is specified.
            prefix = ''

        self._update_object_count()

        objects, object_count, bytes_used = self._object_info
        if not objects:
            return []

        gen_objs = None
        if end_marker:
            gen_objs = filter_end_marker(objects, end_marker)
        else:
            # It is okay to use a list here in place of a generator object
            # since they both work in "for x in ..." loops.
            gen_objs = objects
        if marker and marker >= prefix:
            # If we have a marker, and that marker includes the prefix (in
            # other words (or code) marker.startswith(prefix)), then we must
            # filter by the marker first.
            gen_objs = filter_marker(gen_objs, marker)
            if prefix:
                gen_objs = filter_prefix(gen_objs, prefix)
        elif prefix:
            gen_objs = filter_prefix(gen_objs, prefix)

        if prefix is not None and delimiter:
            gen_objs = filter_delimiter(gen_objs, delimiter, prefix)

        if limit > 0:
            gen_objs = filter_limit(gen_objs, limit)

        container_list = []
        for obj in gen_objs:
            list_item = []
            list_item.append(obj)
            obj_path = os.path.join(self.datadir, obj)
            # Note that here we are reading object metadata, not account or
            # container metadata which requires the timestamp stripped when
            # read (see _read_metadata() above).
            metadata = read_metadata(obj_path)
            if not metadata or not validate_object(metadata):
                metadata = create_object_metadata(obj_path)
            if metadata:
                list_item.append(metadata[X_TIMESTAMP])
                list_item.append(int(metadata[X_CONTENT_LENGTH]))
                list_item.append(metadata[X_CONTENT_TYPE])
                list_item.append(metadata[X_ETAG])
            container_list.append(list_item)

        return container_list

    def _update_object_count(self):
        if not self._object_info:
            self._object_info = get_container_details(self.datadir)

        objects, object_count, bytes_used = self._object_info

        if X_OBJECTS_COUNT not in self.metadata \
                or int(self.metadata[X_OBJECTS_COUNT][0]) != object_count \
                or X_BYTES_USED not in self.metadata \
                or int(self.metadata[X_BYTES_USED][0]) != bytes_used:
            self.metadata[X_OBJECTS_COUNT] = (object_count, 0)
            self.metadata[X_BYTES_USED] = (bytes_used, 0)
            write_metadata(self.datadir, self.metadata)

    def get_info(self, include_metadata=False):
        """
        Get global data for the container.
        :returns: dict with keys: account, container, object_count, bytes_used,
                      hash, id, created_at, put_timestamp, delete_timestamp,
                      reported_put_timestamp, reported_delete_timestamp,
                      reported_object_count, and reported_bytes_used.
                  If include_metadata is set, metadata is included as a key
                  pointing to a dict of tuples of the metadata
        """
        # TODO: delete_timestamp, reported_put_timestamp
        #       reported_delete_timestamp, reported_object_count,
        #       reported_bytes_used, created_at
        if not Glusterfs.OBJECT_ONLY:
            # If we are not configured for object only environments, we should
            # update the object counts in case they changed behind our back.
            self._update_object_count()

        data = {'account' : self.account, 'container' : self.container,
                'object_count' : self.metadata.get(X_OBJECTS_COUNT, ('0', 0))[0],
                'bytes_used' : self.metadata.get(X_BYTES_USED, ('0',0))[0],
                'hash': '', 'id' : '', 'created_at' : '1',
                'put_timestamp' : self.metadata.get(X_PUT_TIMESTAMP, ('0',0))[0],
                'delete_timestamp' : '1',
                'reported_put_timestamp' : '1', 'reported_delete_timestamp' : '1',
                'reported_object_count' : '1', 'reported_bytes_used' : '1'}
        if include_metadata:
            data['metadata'] = self.metadata
        return data

    def update_put_timestamp(self, timestamp):
        """
        Update the PUT timestamp for the container.

        If the container does not exist, create it using a PUT timestamp of
        the given value.

        If the container does exist, update the PUT timestamp only if it is
        later than the existing value.
        """
        if not self._dir_exists:
            do_mkdir(self.datadir)

        if not self.metadata:
            create_container_metadata(self.datadir)
            self.metadata = _read_metadata(self.datadir)

        assert self.metadata

        if timestamp > self.metadata[X_PUT_TIMESTAMP]:
            self.metadata[X_PUT_TIMESTAMP] = (timestamp, 0)
            write_metadata(self.datadir, self.metadata)

    def put_object(self, name, timestamp, size, content_type,
                    etag, deleted=0):
        # This is a NOOP as the act of creating the object on the file system
        # has inserted it into the container.
        return

    def delete_object(self, name, timestamp):
        # This is a NOOP as the act of deleting the object from the file
        # system has remove it from the container.
        return

    def delete_db(self, timestamp):
        """
        Delete the container (directory) if empty.
        """
        if not dir_empty(self.datadir):
            # FIXME: This is a failure condition here!
            return
        rmdirs(self.datadir)

    def set_x_container_sync_points(self):
        # FIXME: What is this method supposed to do?
        return


class DiskAccount(DiskCommon):
    """
    Usage pattern from account/server.py:
        DELETE:
            .is_deleted()
            .delete_db()
        PUT:
            container:
                .pending_timeout
                .db_file
                .initialize()
                .is_deleted()
                .put_container()
            account:
                .db_file
                .initialize()
                .is_status_deleted()
                .is_deleted()
                .update_put_timestamp()
                .is_deleted() ???
                .update_metadata()
        HEAD:
            .pending_timeout
            .stale_reads_ok
            .is_deleted()
            .get_info()
            .get_container_timestamp()
            .metadata
        GET:
            .pending_timeout
            .stale_reads_ok
            .is_deleted()
            .get_info()
            .metadata
            .list_containers_iter()
        POST:
            .is_deleted()
            .update_metadata()
    """
    def __init__(self, root, drive, account, logger):
        super(DiskAccount, self).__init__(root, drive, account, logger)

        self._container_info = None

        # Since accounts should always exist (given an account maps to a
        # gluster volume directly, and the mount has already been checked at
        # the beginning of the REST API handling), just assert that that
        # assumption still holds.
        assert self._dir_exists_read_metadata()
        assert self._dir_exists

        if not self.metadata or not validate_account(self.metadata):
            create_account_metadata(self.datadir)
            self.metadata = _read_metadata(self.datadir)

    def _update_container_count(self):
        if not self._container_info:
            self._container_info = get_account_details(self.datadir)

        containers, container_count = self._container_info

        if X_CONTAINER_COUNT not in self.metadata \
                or int(self.metadata[X_CONTAINER_COUNT][0]) != container_count:
            self.metadata[X_CONTAINER_COUNT] = (container_count, 0)
            write_metadata(self.datadir, self.metadata)

    def list_containers_iter(self, limit, marker, end_marker,
                             prefix, delimiter):
        """
        Return tuple of name, object_count, bytes_used, 0(is_subdir).
        Used by account server.
        """
        assert not delimiter or (len(delimiter) == 1 and ord(delimiter) <= 254)
        if delimiter and not prefix:
            prefix = ''

        self._update_container_count()

        containers, container_count = self._container_info

        if containers:
            containers.sort()

        if containers and prefix:
            containers = filter_prefix(containers, prefix, sorted=True)

        if containers and delimiter:
            containers = filter_delimiter(containers, delimiter, prefix)

        if containers and marker:
            containers = filter_marker(containers, marker)

        if containers and end_marker:
            containers = filter_end_marker(containers, end_marker)

        if containers and limit:
            if len(containers) > limit:
                containers = filter_limit(containers, limit)

        account_list = []
        if containers:
            for cont in containers:
                list_item = []
                metadata = None
                list_item.append(cont)
                cont_path = os.path.join(self.datadir, cont)
                # Note we are reading container metadata which requires
                # handling to strip the extra timestamp stored with each piece
                # of data.
                metadata = _read_metadata(cont_path)
                if not metadata or not validate_container(metadata):
                    metadata = create_container_metadata(cont_path)

                if metadata:
                    list_item.append(metadata[X_OBJECTS_COUNT][0])
                    list_item.append(metadata[X_BYTES_USED][0])
                    list_item.append(0)
                account_list.append(list_item)

        return account_list

    def get_info(self):
        """
        Get global data for the account.
        :returns: dict with keys: account, created_at, put_timestamp,
                  delete_timestamp, container_count, object_count,
                  bytes_used, hash, id
        """
        if not Glusterfs.OBJECT_ONLY:
            # If we are not configured for object only environments, we should
            # update the container counts in case they changed behind our back.
            self._update_container_count()

        data = {'account' : self.account, 'created_at' : '1',
                'put_timestamp' : '1', 'delete_timestamp' : '1',
                'container_count' : self.metadata.get(X_CONTAINER_COUNT, (0,0))[0],
                'object_count' : self.metadata.get(X_OBJECTS_COUNT, (0,0))[0],
                'bytes_used' : self.metadata.get(X_BYTES_USED, (0,0))[0],
                'hash' : '', 'id' : ''}
        return data

    def get_container_timestamp(self, container_name):
        """
        Get the put_timestamp of a container.

        :param container_name: container name

        :returns: put_timestamp of the container
        """
        cont_path = os.path.join(self.datadir, container_name)
        # We are reading the existing metadata directly, instead of relying on
        # the already read values, so that we can fetch the PUT timestamp
        # below. We take care of stripping the timestamp in the pair directly
        # following the read.
        metadata = read_metadata(cont_path)
        return metadata.get(X_PUT_TIMESTAMP, ('0',0))[0] or None

    def update_put_timestamp(self, timestamp):
        # Since accounts always exists at this point, just update the account
        # PUT timestamp if this given timestamp is later than what we already
        # know.
        assert self._dir_exists

        if timestamp > self.metadata[X_PUT_TIMESTAMP][0]:
            self.metadata[X_PUT_TIMESTAMP] = (timestamp, 0)
            write_metadata(self.datadir, self.metadata)

    def delete_db(self, timestamp):
        """
        Deleting an account is a no-op, since accounts are one-to-one mappings
        to gluster volumes.

        FIXME: This means the caller will end up returning a success status
        code for an operation that really should not be allowed. Instead, we
        should modify the account server to not allow the DELETE method, and
        should probably modify the proxy account controller to not allow the
        DELETE method as well.
        """
        return

    def put_container(self, container, put_timestamp, del_timestamp,
                      object_count, bytes_used):
        """
        For account server.
        """
        # FIXME: What should this actually do?
        self.metadata[X_OBJECTS_COUNT] = (0, put_timestamp)
        self.metadata[X_BYTES_USED] = (0, put_timestamp)
        ccnt = self.metadata[X_CONTAINER_COUNT][0]
        self.metadata[X_CONTAINER_COUNT] = (int(ccnt) + 1, put_timestamp)
        self.metadata[X_PUT_TIMESTAMP] = (1, put_timestamp)
        write_metadata(self.datadir, self.metadata)

    def is_status_deleted(self):
        # This function should always return False. Accounts are not created
        # and deleted, they exist if a Gluster volume can be mounted. There is
        # no way to delete accounts, so this could never return True.
        return False
