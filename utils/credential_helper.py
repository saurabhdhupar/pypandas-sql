from typing import Dict

import keyring

from utils import config_helper

__REDSHIFT__ = 'redshift'
__APP_NAME__ = 'pypandasql'
__DELIM__ = '-'


class Credential:

    def __init__(self, service_name: str, user: str, password: str):
        self.service_name = service_name
        self.user = user
        self.password = password


def save_redshift_credentials(user: str, password: str) -> None:
    credential = Credential(__APP_NAME__ + __DELIM__ + __REDSHIFT__, user, password)
    save_credentials(credential)


def get_redshift_credentials(config: Dict) -> Credential:
    service_name = __APP_NAME__ + __DELIM__ + __REDSHIFT__
    user = config_helper.get_redshift_user(config)
    return Credential(service_name=service_name, user=user, password=keyring.get_password(service_name, user))


def save_credentials(credential: Credential) -> None:
    keyring.set_password(credential.service_name, credential.user, credential.password)
