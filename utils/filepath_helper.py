import os

from appdirs import user_config_dir


__REDSHIFT_CONFIG_FLE__ = 'redshift.ini'
__REDSHIFT__ = 'redshift'
__APP_NAME__ = 'pypandasql'


def get_redshift_config_path() -> str:
    config_file_path = os.path.join(user_config_dir(__APP_NAME__), __REDSHIFT_CONFIG_FLE__)
    return config_file_path
