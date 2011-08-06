# -*- coding: utf-8 -*-
"""
key.py

simulate a boto Key object
"""
import logging

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
    
    def set_contents_from_string(self, data, replace=True):
        """
        store the content of the string in the lumberyard
        """

