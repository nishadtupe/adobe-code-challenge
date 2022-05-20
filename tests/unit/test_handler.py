import pytest
from src import app


@pytest.fixture()
def apigw_event():
    """ Generates API GW Event"""

    return {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-east-1',
                         'eventTime': '2022-05-18T17:27:37.076Z', 'eventName': 'ObjectCreated:Copy',
                         'userIdentity': {'principalId': 'AWS:AIDA36OPGSI7UGENRUX4N'},
                         'requestParameters': {'sourceIPAddress': '135.84.42.99'},
                         'responseElements': {'x-amz-request-id': 'JNBGWPGCZATRAVRK',
                                              'x-amz-id-2': 'CWSuVKxAG9ZMsQvjjHqmQyBHZH3KyZTdI/2erso4Jkwf1stM/oHBHsZnlYnkLCz4ClEnyrl8+HynvzAv1xpwkryXBB5fkQfU'},
                         's3': {'s3SchemaVersion': '1.0', 'configurationId': 'c1d45746-b6e9-4ae0-ae27-ba5e156f52d9',
                                'bucket': {'name': 'n-adobe-test', 'ownerIdentity': {'principalId': 'A1MURXB181E0ZI'},
                                           'arn': 'arn:aws:s3:::n-adobe-test'},
                                'object': {'key': 'hits-data/20220518/data.tsv', 'size': 6259,
                                           'eTag': 'c5cf3ac1e540a3a98685b2787cc27059',
                                           'sequencer': '0062852C8902C1CB43'}}}]}


def test_lambda_handler(apigw_event, mocker):
    ret = app.lambda_handler(apigw_event, "")
    bucket = ret['Records'][0]['s3']['bucket']['name']

    assert bucket == 'n-adobe-test'
    # assert "location" in data.dict_keys()
