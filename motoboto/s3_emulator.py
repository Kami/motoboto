# -*- coding: utf-8 -*-
"""
s3_emulator.py

Emulate the functions of the object returned by boto.connect_s3
"""
import logging

from lumberyard.http_connection import HTTPConnection, HTTPRequestError
from lumberyard.http_util import compute_uri

from motoboto.config import Config
from motoboto.bucket import Bucket

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

        self._log.info("requesting %s" % (uri, ))
        try:
            response = self._http_connection.request(method, uri, body=None)
        except HTTPRequestError, instance:
            self._log.error(str(instance))
            raise
        
        self._log.info("reading response")
        cluster_name = response.read()

        return Bucket(
            self._http_connection, 
            bucket_name.decode("utf-8"), 
            cluster_name.strip()
        )

    def get_all_buckets(self):
        method = "GET"
        uri = compute_uri("list_collections")

        self._log.info("requesting %s" % (uri, ))
        try:
            response = self._http_connection.request(method, uri, body=None)
        except HTTPRequestError, instance:
            self._log.error(str(instance))
            raise
        
        self._log.info("reading response")
        data = response.read()

        bucket_list = list()
        for line in data.split("\n"):
            if not "," in line: # last line is blank
                continue
            (collection_name, cluster_name) = line.split(",")
            bucket = Bucket(
                self._http_connection, 
                collection_name.decode("utf-8"), 
                cluster_name
            )
            bucket_list.append(bucket)
        return bucket_list

    def delete_bucket(self, bucket_name):
        method = "GET"
        uri = compute_uri("data", bucket_name, action="delete_collection")

        self._log.info("requesting %s" % (uri, ))
        try:
            response = self._http_connection.request(method, uri, body=None)
        except HTTPRequestError, instance:
            self._log.error(str(instance))
            raise
        
        self._log.info("reading response")
        response.read()

