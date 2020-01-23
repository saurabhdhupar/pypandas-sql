import psycopg2
import sqlalchemy

from pypandas_sql.dbconnector.db_connector import DBConnector
from utils.config_helper import read_redshift_config_file, get_redshift_host, get_redshift_port
from utils.credential_helper import get_redshift_credentials
from utils.filepath_helper import get_redshift_config_path

__CREDENTIALS__ = 'credentials'
__ENGINE_NAME__ = 'redshift+psycopg2'


class RedshiftConnector(DBConnector):

    def __init__(self) -> None:
        config_path = get_redshift_config_path()
        connection_attr = read_redshift_config_file(config_path)
        credentials = get_redshift_credentials(connection_attr)
        connection_attr[__CREDENTIALS__] = credentials
        super(self).__init__(engine_name=__ENGINE_NAME__, connection_attr=connection_attr)

    def get_uri(self, schema: str) -> str:
        credentials = self.connection_attr[__CREDENTIALS__]
        host = get_redshift_host(self.connection_attr)
        port = get_redshift_port(self.connection_attr)
        return f'{self.engine_name}://{credentials.user}:{credentials.password}@{host}:{port}/{schema}'

    def get_engine(self, schema: str):
        return sqlalchemy.create_engine(self.get_uri(schema))

    def get_connection(self, schema: str):
        credentials = self.connection_attr[__CREDENTIALS__]
        return psycopg2.connect(dbname=schema,
                                host=get_redshift_host(self.connection_attr),
                                port=get_redshift_port(self.connection_attr),
                                user=credentials.user,
                                password=credentials.password)

    def get_cursor(self):
        return self.get_connection().cursor()
