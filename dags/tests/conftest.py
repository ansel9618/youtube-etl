import pytest

from airflow.models import DagBag, Variable


@pytest.fixture
def dagbag():
    return DagBag()


@pytest.fixture
def api_key():
    API_KEY = Variable.get("API_KEY")
    return API_KEY


@pytest.fixture
def channel_id():
    CHANNEL_ID = Variable.get("CHANNEL_ID")
    return CHANNEL_ID
