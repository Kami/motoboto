# -*- coding: utf-8 -*-
"""
config.py

configuration information for motoboto
"""
import os
import os.path

#TODO: locate the config in the proper place for the platform
_config_path = os.path.expandvars("$HOME/.motoboto")
_user_name_env = "MOTOBOTO_USER_NAME"
_auth_key_id_env = "MOTOBOTO_AUTH_KEY_ID"
_auth_key_env = "MOTOBOTO_AUTH_KEY"

class Config(object):
    """
    configuration information for motoboto
    """
    def __init__(self):
        self.user_name = None
        self.auth_key_id = None
        self.auth_key = None        

        if os.path.isfile(_config_path):
            self._load_config()

        # environment variables override config file
        if _user_name_env in os.environ:
            self.user_name = os.environ[_user_name_env]
        if _auth_key_id_env in os.environ:
            self.auth_key_id = os.environ[_auth_key_id_env]
        if _auth_key_env in os.environ:
            self.auth_key = os.environ[_auth_key_env]

        assert self.user_name is not None
        assert self.auth_key_id is not None
        assert self.auth_key is not None

    def _load_config(self):
        for line in open(_config_path):
            key, value = line.split()
            if key.lower() in ["user_name", "username"]:
                self.user_name = value
            elif key.lower() in ["auth_key_id", "authkeyid"]:
                self.auth_key_id = value
            elif key.lower() in ["auth_key", "authkey"]:
                self.auth_key = value

