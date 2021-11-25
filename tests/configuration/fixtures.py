import pytest
from tap_mixpanel.client import MixpanelClient


@pytest.fixture
def mixpanel_client():
    # Support of request_timeout have been added. So, now MixpanelClient accept request_timeout parameter which is mandatory
    mixpanel_client = MixpanelClient('API_SECRET', api_domain="mixpanel.com", request_timeout=1) # Pass extra request_timeout parameter
    mixpanel_client._MixpanelClient__verified = True
    return mixpanel_client
