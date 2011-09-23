# -*- coding: utf-8 -*-
"""
s3_emulator.py

Emulate the functions of the object returned by boto.connect_s3
"""
import json
import logging

from lumberyard.http_connection import HTTPConnection, HTTPRequestError
from lumberyard.http_util import compute_default_hostname, \
        compute_default_collection_name, \
        compute_uri

from motoboto.config import load_config_from_environment, load_config_from_file
from motoboto.s3.bucket import Bucket

class S3Emulator(object):
    """
    Emulate the functions of the object returned by boto.connect_s3
    """
    def __init__(self, config=None):
        self._log = logging.getLogger("S3Emulator")

        if config is not None:
            self._config = config
        else:
            config = load_config_from_environment()
            if config is not None:
                self._config = config
            else:
                config = load_config_from_file()
                if config is not None:
                    self._config = config
                else:
                    raise ValueError(
                        "You must specify config in environment of file"
                    )

        self._default_bucket = Bucket(
            self._config, 
            compute_default_collection_name(self._config.user_name)
        )

    def close(self):
        self._log.debug("closing")

    def create_bucket(self, bucket_name):
        method = "POST"

        http_connection = HTTPConnection(
            compute_default_hostname(),
            self._config.user_name,
            self._config.auth_key,
            self._config.auth_key_id
        )
        uri = compute_uri(
            "/".join(["customers", self._config.user_name, "collections"]), 
            action="create",
            name=bucket_name
        )

        self._log.info("requesting %s" % (uri, ))
        try:
            response = http_connection.request(method, uri, body=None)
        except HTTPRequestError, instance:
            self._log.error(str(instance))
            http_connection.close()
            raise
        
        response.read()
        http_connection.close()

        return Bucket(self._config, bucket_name.decode("utf-8"))

    def get_all_buckets(self):
        method = "GET"

        http_connection = HTTPConnection(
            compute_default_hostname(),
            self._config.user_name,
            self._config.auth_key,
            self._config.auth_key_id
        )
        uri = compute_uri(
            "/".join(["customers", self._config.user_name, "collections"]), 
        )

        self._log.info("requesting %s" % (uri, ))
        try:
            response = http_connection.request(method, uri, body=None)
        except HTTPRequestError, instance:
            self._log.error(str(instance))
            http_connection.close()
            raise
        
        self._log.info("reading response")
        data = response.read()
        http_connection.close()
        collection_list = json.loads(data)

        bucket_list = list()
        for collection_name, _timestamp in collection_list:
            bucket = Bucket(self._config, collection_name.decode("utf-8"))
            bucket_list.append(bucket)
        return bucket_list

    def delete_bucket(self, bucket_name):
        method = "DELETE"

        http_connection = HTTPConnection(
            compute_default_hostname(),
            self._config.user_name,
            self._config.auth_key,
            self._config.auth_key_id
        )

        if bucket_name.startswith("/"):
            bucket_name = bucket_name[1:]
        uri = compute_uri(
            "/".join([
                "customers", 
                self._config.user_name, 
                "collections",
                bucket_name
            ]), 
        )

        self._log.info("requesting %s" % (uri, ))
        try:
            response = http_connection.request(method, uri, body=None)
        except HTTPRequestError, instance:
            self._log.error(str(instance))
            http_connection.close()
            raise
        
        response.read()
        http_connection.close()

