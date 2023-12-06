import unittest
from unittest import mock
from tests.unittests.test_error_handling import MockResponse
from tap_mixpanel.client import MixpanelClient, MixpanelForbiddenError

class TestServiceAccountAuthentication(unittest.TestCase):
    """
    Test that tap do authentication with service account credentials without any error if it is provided.
    """

    @mock.patch("tap_mixpanel.client.MixpanelClient.check_access")
    def test_token_creds(self, mock_check_access):
        """Test authentication with token credentials(api_secret).

        Args:
            mock_check_access: Mock the check_access method to test authentication.
        """
        with MixpanelClient("api_secret", None, None, None,"api_domain", 300) as client_:
            pass
        
        self.assertEqual(client_.auth_header, "Basic YXBpX3NlY3JldA==")

    @mock.patch("tap_mixpanel.client.MixpanelClient.check_access")
    def test_service_account_creds(self, mock_check_access):
        """Test authentication with service account credentials(username, secret).

        Args:
            mock_check_access: Mock the check_access method to test authentication.
        """
        with MixpanelClient(None, "service_account_username", "service_account_secret", "project_id","api_domain", 300, auth_type="saa") as client_:
            pass
        
        self.assertEqual(client_.auth_header, "Basic c2VydmljZV9hY2NvdW50X3VzZXJuYW1lOnNlcnZpY2VfYWNjb3VudF9zZWNyZXQ=")

    @mock.patch("tap_mixpanel.client.MixpanelClient.check_access")
    def test_no_creds(self, mock_check_access):
        """Test that tap throws an error if credentials is not provided.

        Args:
            mock_check_access: Mock the check_access method to test authentication.
        """
        with self.assertRaises(Exception) as e:
            with MixpanelClient(None, None, None, None,"api_domain", 300) as client_:
                pass
        
        self.assertEqual(str(e.exception), "Error: Missing api_secret or service account username/secret in tap config.json")

    @mock.patch("requests.Session.request", return_value = MockResponse(403))
    @mock.patch("tap_mixpanel.client.LOGGER.error")
    def test_check_access_403_error_for_service_account_creds(self, mock_logger, mock_request):
        """Test that tap handles 403 error with proper message.

        Args:
            mock_logger: Mock of LOGGER to verify the logger message
            mock_request: Mock Session.request to explicitly raise the forbidden(403) error.
        """
        with self.assertRaises(MixpanelForbiddenError):
            with MixpanelClient(None, "service_account_username", "service_account_secret", "project_id","api_domain", 300, auth_type="saa") as client_:
                    client_.check_access()
    
        mock_logger.assert_called_with('HTTP-error-code: 403, Error: User is not a member of this project: %s or this project is invalid', 'project_id')
