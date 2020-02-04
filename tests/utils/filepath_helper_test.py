import os
import unittest

from mock import patch, MagicMock

from utils.filepath_helper import get_redshift_config_path


class FileHelperTest(unittest.TestCase):

    @patch('appdirs.user_config_dir', return_value=os.path.dirname(__file__))
    def test_get_redshift_config_path(self, mock_user_config_dir: MagicMock):
        path = get_redshift_config_path()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        self.assertEqual(path, os.path.join(os.path.dirname(__file__), 'redshift.ini'))
