from typing import Any, Optional

import psycopg2
import sqlalchemy

from pypandas_sql.dbconnector.db_connector import DBConnector
from utils import config_helper, credential_helper, filepath_helper

__CREDENTIALS__ = 'credentials'
__ENGINE_NAME__ = 'redshift+psycopg2'


class RedshiftConnector(DBConnector):

    def __init__(self) -> None:
        config_path = filepath_helper.get_redshift_config_path()
        connection_attr = config_helper.read_redshift_config_file(config_path)
        credentials = credential_helper.get_redshift_credentials(connection_attr)
        connection_attr[__CREDENTIALS__] = credentials
        super(RedshiftConnector, self).__init__(engine_name=__ENGINE_NAME__, connection_attr=connection_attr)

    def get_uri(self, schema: Optional[str]) -> str:
        assert schema is not None and len(schema) > 0
        credentials = self.connection_attr[__CREDENTIALS__]
        host = config_helper.get_redshift_host(self.connection_attr)
        port = config_helper.get_redshift_port(self.connection_attr)
        return f'{self.engine_name}://{credentials.user}:{credentials.password}@{host}:{port}/{schema}'

    def get_engine(self, schema: Optional[str]) -> Any:
        assert schema is not None and len(schema) > 0
        return sqlalchemy.create_engine(self.get_uri(schema))

    def get_connection(self, schema: Optional[str]) -> Any:
        assert schema is not None and len(schema) > 0
        credentials = self.connection_attr[__CREDENTIALS__]
        return psycopg2.connect(dbname=schema,
                                host=config_helper.get_redshift_host(self.connection_attr),
                                port=config_helper.get_redshift_port(self.connection_attr),
                                user=credentials.user,
                                password=credentials.password)

    def get_cursor(self, schema: Optional[str]) -> Any:
        assert schema is not None and len(schema) > 0
        return self.get_connection(schema).cursor()
