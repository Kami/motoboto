# -*- coding: utf-8 -*-
"""
key.py

simulate a boto Key object
"""
import logging

from lumberyard.http_connection import HTTPRequestError
from lumberyard.http_util import compute_uri

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

    @property
    def name(self):
        """key name."""
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

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
            stat_result = response.read()
            self._log.debug("stat result = '%s'" % (stat_result, ))

        return found

    def set_contents_from_string(self, data, replace=True):
        """
        store the content of the string in the lumberyard
        """

    def get_contents_as_string(self):
        """
        return the contents from lumberyard as a string
        """

