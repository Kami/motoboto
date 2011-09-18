# -*- coding: utf-8 -*-
"""
config.py

configuration information for motoboto
"""
from collections import namedtuple
import os
import os.path

config_template = namedtuple(
    "Config", ["user_name", "auth_key_id", "auth_key"]
)

#TODO: locate the config in the proper place for the platform
_config_path = os.path.expandvars("$HOME/.motoboto")
_user_name_env = "MOTOBOTO_USER_NAME"
_auth_key_id_env = "MOTOBOTO_AUTH_KEY_ID"
_auth_key_env = "MOTOBOTO_AUTH_KEY"


def load_config_from_environment():
    if not _user_name_env in os.environ:
        return None
    if not _auth_key_id_env in os.environ:
        return None
    if not _auth_key_env in os.environ:
        return None

    return config_template(
        user_name=os.environ[_user_name_env],
        auth_key_id=os.environ[_auth_key_id_env],
        auth_key=os.environ[_auth_key_env]
    )

def load_config_from_file(path=_config_path):
    user_name = None
    auth_key_id = None
    auth_key = None

    for line in open(path):
        line = line.strip()
        if len(line) == 0:
            continue
        if line[0] == "#":
            continue
        key, value = line.split()
        if key.lower() in ["user_name", "username"]:
            user_name = value
        elif key.lower() in ["auth_key_id", "authkeyid"]:
            auth_key_id = value
        elif key.lower() in ["auth_key", "authkey"]:
            auth_key = value

    if user_name == None:
        return None
    if auth_key_id == None:
        return None
    if auth_key == None:
        return None

    return config_template(
        user_name=user_name,
        auth_key_id=auth_key_id,
        auth_key=auth_key
    )

