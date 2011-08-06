# -*- coding: utf-8 -*-
"""
s3_emulator.py

Emulate the functions of the object returned by boto.connect_s3
"""
import logging

from lumberyard.http_connection import HTTPConnection
from lumberyard.http_util import compute_uri

from motoboto.config import Config

class S3Emulator(object):
    """
    Emulate the functions of the object returned by boto.connect_s3
    """
    def __init__(self):
        self._log = logging.getLogger("S3Emulator")
        config = Config()
        self._http_connection = HTTPConnection(
            config.base_address,
            config.user_name,
            config.auth_key,
            config.auth_key_id
        )

    def close(self):
        self._log.debug("closing")
        self._http_connection.close()

    def create_bucket(self, bucket_name):
        method = "GET"
        uri = compute_uri("data", bucket_name, action="create_collection")

        log.info("requesting %s" % (uri, ))
        try:
            response = self._http_connection.request(method, uri, body=None)
        except HTTPRequestError, instance:
            log.error(str(instance))
            raise
        
        log.info("reading response")
        response.read()

    def get_all_buckets(self):
        method = "GET"
        uri = compute_uri("list_collections")

        log.info("requesting %s" % (uri, ))
        try:
            response = self._http_connection.request(method, uri, body=None)
        except HTTPRequestError, instance:
            log.error(str(instance))
            raise
        
        log.info("reading response")
        return response.read()

    def delete_bucket(bucket_name):
        method = "GET"
        uri = compute_uri("data", bucket_name, action="delete_collection")

        log.info("requesting %s" % (uri, ))
        try:
            response = self._http_connection.request(method, uri, body=None)
        except HTTPRequestError, instance:
            log.error(str(instance))
            raise
        
        log.info("reading response")
        response.read()

