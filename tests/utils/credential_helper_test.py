import unittest

from mock import patch, MagicMock

from utils.credential_helper import save_redshift_credentials, get_redshift_credentials, save_credentials, Credential


class CredentialTest(unittest.TestCase):

    @patch('keyring.set_password')
    def test_save_redshift_credentials(self, mock_set_password: MagicMock):
        save_redshift_credentials('test', 'randompassword')
        mock_set_password.assert_called_once_with('pypandasql-redshift', 'test', 'randompassword')

    @patch('keyring.set_password')
    def test_save_credentials(self, mock_set_password: MagicMock):
        credential = Credential('pypandasql-redshift', 'test', 'randompassword')
        save_credentials(credential)

    @patch('keyring.get_password', return_value='randompassword')
    def test_get_redshift_credentials(self, mock_get_password: MagicMock):
        config = {'user_name': 'test'}
        credential = get_redshift_credentials(config)
        mock_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        self.assertEqual(credential.service_name, 'pypandasql-redshift')
        self.assertEqual(credential.user, 'test')
        self.assertEqual(credential.password, 'randompassword')
