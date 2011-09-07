# -*- coding: utf-8 -*-
"""
bucket.py

simulate a boto Bucket object
"""
import logging

from lumberyard.http_util import compute_hostname
from lumberyard.http_connection import HTTPConnection

from motoboto.s3.key import Key

class Bucket(object):
    """
    simulate a boto Bucket object
    """
    def __init__(self, config, collection_name):
        self._log = logging.getLogger("Bucket(%s)" % (collection_name, ))
        self._config = config
        self._collection_name = collection_name

    @property
    def name(self):
        return self._collection_name
    
    def get_key(self, name):
        """return a key object for the name"""
        return Key(bucket=self, name=name)
    
    def create_http_connection(self):
        """
        create an HTTP connection with our name as the host
        """
        return HTTPConnection(
            compute_hostname(self._collection_name),
            self._config.user_name,
            self._config.auth_key,
            self._config.auth_key_id
        )

