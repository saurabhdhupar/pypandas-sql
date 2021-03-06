import errno
import os
from typing import Dict
from configparser import ConfigParser

__REDSHIFT_CONFIG_FLE__ = 'redshift.ini'
__REDSHIFT__ = 'redshift'
__MYSQL__ = 'mysql'
__APP_NAME__ = 'pypandasql'
__HOST__ = 'host'
__PORT__ = 'port'
__SSL__ = 'ssl'
__USER_NAME = 'user_name'
__SSL_CERT__ = 'ssl-cert'
__SSL_CA__ = 'ssl-ca'
__SSL_KEY__ = 'ssl-key'


def write_config_file(config: ConfigParser, file_path: str) -> None:
    if not os.path.exists(os.path.dirname(file_path)):
        try:
            os.makedirs(os.path.dirname(file_path))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
    try:
        with open(file_path, 'w') as config_file:
            config.write(config_file)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise


def read_config_file(file_path: str) -> ConfigParser:
    config = ConfigParser()
    if not os.path.isfile(file_path):
        raise FileNotFoundError("Missing Config File")
    config.read(file_path)
    return config


def get_redshift_user(config: Dict) -> str:
    return config[__USER_NAME]


def get_redshift_host(config: Dict) -> str:
    return config[__HOST__]


def get_redshift_port(config: Dict) -> int:
    return int(config[__PORT__])


def get_mysql_user(config: Dict) -> str:
    return config[__USER_NAME]


def get_mysql_host(config: Dict) -> str:
    return config[__HOST__]


def get_mysql_port(config: Dict) -> int:
    return int(config[__PORT__])


def read_redshift_config_file(file_path: str) -> Dict:
    try:
        config = read_config_file(file_path)
        config_prop = {}
        properties_list = [__HOST__, __PORT__, __USER_NAME]
        for prop in properties_list:
            config_prop[prop] = config[__REDSHIFT__][prop]
        return config_prop
    except FileNotFoundError:
        raise FileNotFoundError("Redshift Config File Not Found. Use 'pypandasql configure' to use this method")


def read_mysql_config_file(file_path: str) -> Dict:
    try:
        config = read_config_file(file_path)
        config_prop = dict()
        properties_list = [__HOST__, __PORT__, __USER_NAME]
        for prop in properties_list:
            config_prop[prop] = config[__MYSQL__][prop]
        ssl_properties_list = [__SSL_CERT__, __SSL_CA__, __SSL_KEY__]
        for ssl_prop in ssl_properties_list:
            if ssl_prop in config[__MYSQL__]:
                config_prop[ssl_prop] = config[__MYSQL__][ssl_prop]
        return config_prop
    except FileNotFoundError:
        raise FileNotFoundError("Redshift Config File Not Found. Use 'pypandasql configure' to use this method")


def write_redshift_config_file(file_path: str, host: str, port: int, user: str) -> None:
    config = ConfigParser()
    config[__REDSHIFT__] = {}
    config[__REDSHIFT__][__HOST__] = host
    config[__REDSHIFT__][__PORT__] = str(port)
    config[__REDSHIFT__][__USER_NAME] = user
    write_config_file(config, file_path)


def write_mysql_config_file(file_path: str, host: str, port: int, user: str, ssl_cert_path: str,
                            ssl_ca_path: str, ssl_key_path: str) -> None:
    config = ConfigParser()
    config[__MYSQL__] = {}
    config[__MYSQL__][__HOST__] = host
    config[__MYSQL__][__PORT__] = str(port)
    config[__MYSQL__][__USER_NAME] = user
    if ssl_cert_path or ssl_ca_path or ssl_key_path:
        config[__MYSQL__][__SSL_CERT__] = ssl_cert_path
        config[__MYSQL__][__SSL_CA__] = ssl_ca_path
        config[__MYSQL__][__SSL_KEY__] = ssl_key_path
    write_config_file(config, file_path)
