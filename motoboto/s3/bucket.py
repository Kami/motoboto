# -*- coding: utf-8 -*-
"""
bucket.py

simulate a boto Bucket object
"""
import json
import logging

from lumberyard.http_util import compute_default_hostname, \
        compute_collection_hostname, \
        compute_uri
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

    def get_all_keys(self):
        """return a list of all keys in this bucket"""
        method = "GET"

        http_connection = self.create_http_connection()

        uri = compute_uri("data/")

        response = http_connection.request(method, uri)
        
        data = response.read()
        http_connection.close()
        data_list = json.loads(data)
        return [Key(bucket=self, name=n) for n in data_list]
    
    def get_key(self, name):
        """return a key object for the name"""
        return Key(bucket=self, name=name)
    
    def create_http_connection(self):
        """
        create an HTTP connection with our name as the host
        """
        return HTTPConnection(
            compute_collection_hostname(self._collection_name),
            self._config.user_name,
            self._config.auth_key,
            self._config.auth_key_id
        )

    def get_space_used(self):
        """
        get disk space statistics for this bucket
        """

        http_connection = HTTPConnection(
            compute_default_hostname(),
            self._config.user_name,
            self._config.auth_key,
            self._config.auth_key_id
        )
        method = "GET"
        uri = compute_uri(
            "/".join([
                "customers", 
                self._config.user_name, 
                "collections",
                self._collection_name
            ]),
            action="space_usage"
        )

        response = http_connection.request(method, uri)
        data = response.read()
        http_connection.close()
    
        return json.loads(data)

