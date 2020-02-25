from typing import Optional, Any, Dict

from utils import config_helper, credential_helper, filepath_helper
from pypandas_sql.dbconnector.db_connector import DBConnector

import MySQLdb

from utils.credential_helper import Credential

__CREDENTIALS__ = 'credentials'
__ENGINE_NAME__ = 'mysql+pymysql'


class MySQLConnector(DBConnector):

    def __init__(self) -> None:
        config_path = filepath_helper.get_mysql_config_path()
        connection_attr = config_helper.read_mysql_config_file(config_path)
        credentials = credential_helper.get_mysql_credentials(connection_attr)
        connection_attr[__CREDENTIALS__] = credentials
        super(MySQLConnector, self).__init__(engine_name=__ENGINE_NAME__, connection_attr=connection_attr)

    def get_uri(self, schema: Optional[str]) -> str:
        assert schema is not None and len(schema) > 0
        credentials = self.connection_attr[__CREDENTIALS__]
        host = config_helper.get_mysql_user(self.connection_attr)
        port = config_helper.get_mysql_port(self.connection_attr)
        return f'{self.engine_name}://{credentials.user}:{credentials.password}@{host}:{port}/{schema}'

    def get_config_params_dict(self, schema: Optional[str], credentials: Credential) -> Dict:
        config_params = {
            "host": config_helper.get_redshift_host(self.connection_attr),
            "port": config_helper.get_redshift_port(self.connection_attr),
            "user": credentials.user,
            "password": credentials.password
        }
        if schema:
            config_params['db'] = schema
        if 'ssl-cert' in self.connection_attr:
            config_params['ssl'] = {
                "cert": self.connection_attr['ssl-cert'],
                "ca": self.connection_attr['ssl-ca'],
                "key": self.connection_attr['ssl-key']
            }
        return config_params

    def get_connection(self, schema: Optional[str]) -> Any:
        assert schema is not None and len(schema) > 0
        credentials = self.connection_attr[__CREDENTIALS__]
        config_params = self.get_config_params_dict(schema, credentials)
        return MySQLdb.connect(**config_params)
