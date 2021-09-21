from collections.abc import Generator
from unittest import mock
from unittest.mock import patch

import backoff
import requests
import requests_mock
from pytest import raises
from tap_mixpanel import client
from tap_mixpanel.client import (ReadTimeoutError, Server5xxError,
                                 Server429Error, MixpanelInternalServiceError)
from tests.configuration.fixtures import mixpanel_client


@mock.patch('time.sleep', return_value=None)
def test_request_export_backoff_on_timeout(mock_sleep, mixpanel_client):
    with requests_mock.Mocker() as m:
        m.request('GET', 'http://test.com',
                  exc=requests.exceptions.Timeout('Timeout on request'))

        with raises(ReadTimeoutError) as ex:
            for record in mixpanel_client.request_export('GET', url='http://test.com'):
                pass
        # Assert backoff retry count as expected
        assert mock_sleep.call_count == client.BACKOFF_MAX_TRIES_REQUEST - 1


@mock.patch('time.sleep', return_value=None)
def test_request_export_backoff_on_remote_timeout(mock_sleep, mixpanel_client):
    with requests_mock.Mocker() as m:
        m.request('GET', 'http://test.com', text=None, status_code=504)
        result = mixpanel_client.request_export('GET', url='http://test.com')

        with raises(Server5xxError) as ex:
            for record in result:
                pass
        # Assert backoff retry count as expected
        assert mock_sleep.call_count == client.BACKOFF_MAX_TRIES_REQUEST - 1


@mock.patch('time.sleep', return_value=None)
def test_perform_request_backoff_on_remote_timeout_429(mock_sleep, mixpanel_client):
    with requests_mock.Mocker() as m:
        m.request('GET', 'http://test.com', text=None,
                  content=b'error', status_code=429)

        with raises(Server429Error) as ex:
            result = mixpanel_client.perform_request(
                'GET', url='http://test.com')
            for record in result:
                pass
        # Assert backoff retry count as expected
        assert mock_sleep.call_count == client.BACKOFF_MAX_TRIES_REQUEST - 1


@mock.patch('time.sleep', return_value=None)
def test_perform_request_backoff_on_remote_timeout_500(mock_sleep, mixpanel_client):
    with requests_mock.Mocker() as m:
        m.request('GET', 'http://test.com', text=None, status_code=500)

        with raises(MixpanelInternalServiceError) as ex:
            result = mixpanel_client.perform_request(
                'GET', url='http://test.com')

            for record in result:
                pass
        # Assert backoff retry count as expected
        assert mock_sleep.call_count == client.BACKOFF_MAX_TRIES_REQUEST - 1


@mock.patch('time.sleep', return_value=None)
def test_check_access_backoff_on_remote_timeout_429(mock_sleep, mixpanel_client):
    with requests_mock.Mocker() as m:
        m.request('GET', 'https://mixpanel.com/api/2.0/engage',
                  content=b'error', text=None, status_code=429)

        with raises(Server429Error) as ex:
            result = mixpanel_client.check_access()
        # Assert backoff retry count as expected
        assert mock_sleep.call_count == 5 - 1


@mock.patch('time.sleep', return_value=None)
def test_check_access_backoff_on_remote_timeout_500(mock_sleep, mixpanel_client):
    with requests_mock.Mocker() as m:
        m.request('GET', 'https://mixpanel.com/api/2.0/engage',
                  content=b'error', text=None, status_code=500)

        with raises(MixpanelInternalServiceError) as ex:
            result = mixpanel_client.check_access()
        # Assert backoff retry count as expected
        assert mock_sleep.call_count == 5 - 1


@mock.patch('time.sleep', return_value=None)
def test_request_backoff_on_timeout(mock_sleep, mixpanel_client):
    with requests_mock.Mocker() as m:
        m.request('GET', 'http://test.com',
                  exc=requests.exceptions.Timeout('Timeout on request'))

        with raises(ReadTimeoutError) as ex:
            result = mixpanel_client.request('GET', url='http://test.com')
        # Assert backoff retry count as expected
        assert mock_sleep.call_count == client.BACKOFF_MAX_TRIES_REQUEST - 1


def test_request_returns_json(mixpanel_client):
    with requests_mock.Mocker() as m:
        m.request('GET', 'http://test.com', json={'a': 'b'})
        result = mixpanel_client.request('GET', url='http://test.com')
        assert result == {'a': 'b'}


def test_request_export_returns_generator(mixpanel_client):
    with requests_mock.Mocker() as m:
        m.request('GET', 'http://test.com', json={'a': 'b'})
        result = mixpanel_client.request_export('GET', url='http://test.com')
        assert isinstance(result, Generator)
