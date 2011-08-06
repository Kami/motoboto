# -*- coding: utf-8 -*-
"""
bucket.py

simulate a boto Bucket object
"""
import logging

class Bucket(object):
    """
    simulate a boto Bucket object
    """
    def __init__(self, http_connection, collection_name, cluster_name):
        self._log = logging.getLogger("Bucket(%s)" % (collection_name, ))
        self._http_connection = http_connection
        self._collection_name = collection_name
        self._cluster_name = cluster_name

    @property
    def name(self):
        return self._collection_name

    
