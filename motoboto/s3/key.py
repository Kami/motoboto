# -*- coding: utf-8 -*-
"""
key.py

simulate a boto Key object
"""
import logging
import os

from lumberyard.http_connection import HTTPRequestError
from lumberyard.http_util import compute_uri
from lumberyard.read_reporter import ReadReporter

from motoboto.s3.archive_callback_wrapper import ArchiveCallbackWrapper
from motoboto.s3.retrieve_callback_wrapper import NullCallbackWrapper, \
        RetrieveCallbackWrapper

_read_buffer_size = 64 * 1024

class Key(object):
    """
    simulate a boto Key object
    """
    def __init__(self, bucket=None, name=None):
        self._log = logging.getLogger("Key")
        self._bucket = bucket
        self._name = name

    def close(self):
        self._log.debug("closing")

    def _get_name(self):
        """key name."""
        return self._name

    def _set_name(self, value):
        self._name = value

    name = property(_get_name, _set_name)

    def exists(self):
        """
        return True if we can stat the key
        """  
        found = False
        method = "GET"
        uri = compute_uri("data", self._name, action="stat")

        self._log.info("requesting %s" % (uri, ))
        try:
            response = self._bucket.http_connection.request(
                method, uri, body=None
            )
        except HTTPRequestError, instance:
            if instance.status == 404: # not found
                pass
            else:
                self._log.error(str(instance))
                raise
        else:
            found = True
        
        if found:
            self._log.info("reading response")
            response.read()

        return found

    def set_contents_from_string(
        self, data, replace=True, cb=None, cb_count=10
    ):
        """
        store the content of the string in the lumberyard
        """
        # 2011-08-07 dougfort -- If they don't want to replace,
        # stop them right here.
        if not replace:
            if self.exists():
                raise KeyError("attempt to replace key %r" % (self._name))

        method = "POST"
        uri = compute_uri("data", self._name)

        self._log.info("posting %s" % (uri, ))
        response = self._bucket.http_connection.request(
            method, uri, body=data
        )
        
        self._log.info("reading response")
        response.read()

    def set_contents_from_file(
        self, file_object, replace=True, cb=None, cb_count=10
    ):
        """
        store the content of the file in lumberyard
        """
        # 2011-08-07 dougfort -- If they don't want to replace,
        # stop them right here.
        if not replace:
            if self.exists():
                raise KeyError("attempt to replace key %r" % (self._name))

        wrapper = None
        if cb is None:
            body = file_object
        else:
            body = ReadReporter(file_object)
            wrapper = ArchiveCallbackWrapper(body, cb, cb_count) 

        method = "POST"
        uri = compute_uri("data", self._name)

        self._log.info("posting %s" % (uri, ))
        response = self._bucket.http_connection.request(method, uri, body=body)
        
        self._log.info("reading response")
        response.read()

    def get_contents_as_string(self, cb=None, cb_count=10):
        """
        return the contents from lumberyard as a string
        """
        method = "GET"
        uri = compute_uri("data", self._name)

        self._log.info("requesting %s" % (uri, ))
        response = self._bucket.http_connection.request(
            method, uri, body=None
        )
        
        self._log.info("reading response")
        body_list = list()
        while True:
            data = response.read(_read_buffer_size)
            if len(data) == 0:
                break
            body_list.append(data)

        return "".join(body_list)

    def get_contents_to_file(self, file_object, cb=None, cb_count=10):
        """
        return the contents from lumberyard to a file
        """
        method = "GET"
        uri = compute_uri("data", self._name)

        self._log.info("requesting %s" % (uri, ))
        response = self._bucket.http_connection.request(
            method, uri, body=None
        )

        if cb is None:
            reporter = NullCallbackWrapper()
        else:
            reporter = RetrieveCallbackWrapper(self.size, cb, cb_count) 
        
        self._log.info("reading response")
        reporter.start()
        while True:
            data = response.read(_read_buffer_size)
            bytes_read = len(data)
            if bytes_read == 0:
                break
            file_object.write(data)
            reporter.bytes_written(bytes_read)
        reporter.finish()

    def delete(self):
        """
        delete this key from the system
        """
        method = "DELETE"
        uri = compute_uri("data", self._name)

        self._log.info("requesting delete %s" % (uri, ))
        response = self._bucket.http_connection.request(
            method, uri, body=None
        )
        
        self._log.info("reading response")
        response.read()

