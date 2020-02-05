import os
from configparser import ConfigParser

from pyfakefs.fake_filesystem_unittest import TestCase

from utils.config_helper import read_config_file, read_redshift_config_file, get_redshift_port, get_redshift_host, \
    get_redshift_user, write_redshift_config_file, write_config_file


class ConfigHelperTest(TestCase):
    config_path = os.path.join(os.path.dirname(__file__), 'redshift.ini')

    def setUp(self) -> None:
        self.setUpPyfakefs()
        self.fs.add_real_file(self.config_path)

    def test_read_config_file(self):
        file_path = '/test/testconfig.ini'
        with open(self.config_path) as f:
            config_contents = f.read()
        self.fs.create_file(file_path, contents=config_contents)
        config = read_config_file(file_path)
        self.assertIsNotNone(config)

    def test_read_config_file_exception(self):
        file_path = '/test/testconfig.ini'
        with self.assertRaises(FileNotFoundError):
            read_config_file(file_path)

    def test_read_redshift_config_file(self):
        file_path = '/test/testconfig.ini'
        with open(self.config_path) as f:
            config_contents = f.read()
        self.fs.create_file(file_path, contents=config_contents)
        config = read_redshift_config_file(file_path)
        self.assertIsNotNone(config)
        self.assertEqual(config['host'], 'test-host')
        self.assertEqual(config['port'], '1234')
        self.assertEqual(config['user_name'], 'test')

    def test_read_redshift_config_file_exception(self):
        file_path = '/test/testconfig.ini'
        with self.assertRaises(FileNotFoundError):
            read_redshift_config_file(file_path)

    def test_get_redshift_user(self):
        file_path = '/test/testconfig.ini'
        with open(self.config_path) as f:
            config_contents = f.read()
        self.fs.create_file(file_path, contents=config_contents)
        config = read_redshift_config_file(file_path)
        self.assertEqual(get_redshift_user(config), 'test')

    def test_get_redshift_host(self):
        file_path = '/test/testconfig.ini'
        with open(self.config_path) as f:
            config_contents = f.read()
        self.fs.create_file(file_path, contents=config_contents)
        config = read_redshift_config_file(file_path)
        self.assertEqual(get_redshift_host(config), 'test-host')

    def test_get_redshift_port(self):
        file_path = '/test/testconfig.ini'
        with open(self.config_path) as f:
            config_contents = f.read()
        self.fs.create_file(file_path, contents=config_contents)
        config = read_redshift_config_file(file_path)
        self.assertEqual(get_redshift_port(config), 1234)

    def test_write_redshift_config_file(self):
        file_path = '/test/testconfig.ini'
        write_redshift_config_file(file_path, host='test-host', user='test', port=1234)
        config = read_redshift_config_file(file_path)
        self.assertEqual(config['host'], 'test-host')
        self.assertEqual(config['port'], '1234')
        self.assertEqual(config['user_name'], 'test')

    def test_write_config_file_exception(self):
        file_path = '/test1/testconfig.ini'
        self.fs.set_disk_usage(0)
        self.assertFalse(os.path.exists(os.path.dirname(file_path)))
        self.assertFalse(os.path.exists(file_path))
        with self.assertRaises(OSError):
            write_config_file(ConfigParser(), file_path)

    def test_write_config_file(self):
        file_path = '/test1/testconfig.ini'
        self.assertFalse(os.path.exists(os.path.dirname(file_path)))
        self.assertFalse(os.path.exists(file_path))
        config = ConfigParser()
        config['test'] = {'value': 'testval'}
        write_config_file(config, file_path)
        self.assertTrue(os.path.exists(os.path.dirname(file_path)))
        self.assertTrue(os.path.exists(file_path))
        config_written = read_config_file(file_path)
        self.assertEqual(config_written['test']['value'], 'testval')
