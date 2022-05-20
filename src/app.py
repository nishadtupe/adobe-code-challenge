import json
import logging
import os
import re
from datetime import datetime
from io import StringIO

import boto3
import numpy as np
import pandas as pd

# initiate logger
log_level = logging.INFO
if 'LOG_LEVEL' in os.environ:
    if os.environ['LOG_LEVEL'].upper() == 'DEBUG':
        log_level = logging.DEBUG
log_format = "%(asctime)s.%(msecs)d\t- %(levelname)s:%(module)s:%(message)s"
log_stamp = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(format=log_format, datefmt=log_stamp, level=log_level)
logger = logging.getLogger(os.path.basename('adobe.ipynb'))
logger.setLevel(log_level)

# global variables
destination_bucket = os.getenv("DEST_BUCKET", 'n-adobe-test')
s3_client = boto3.client('s3')


class HitsAnalyze:
    """
    RevenueCalculation is meant to calculate the revenue from the hit level data
    """

    def __init__(self, response_object, **kwargs):
        self.region_name = 'us-east-1'
        self.s3_client = boto3.client('s3', region_name=self.region_name)
        self.s3_resource = boto3.resource('s3', region_name=self.region_name)
        # self.processing_date = datetime.now().astimezone()
        self.file_name = "analytics/" + datetime.now().strftime("%Y-%m-%d") + "_SearchKeywordPerformance.tab"
        # self.destination_path =
        self.resp = response_object

    def _extract_revenue(self, product_list):

        """ extracts revenue from the product_list - comma seperate string.
        Args:
            product_list (string) : product_list nested string object
        Raises:
            e: Exception
        Returns:
            string : product revenue
        """
        try:
            revenue = product_list.split(';')[3]
            return revenue
        except Exception as e:
            logger.error("Issue with revenue extraction check the logs ..")
            logger.error("e")

    def _extract_searchwords(self, referrer_string):

        """ extracts keywords from the referrer URL
        Args:
            referrer URL (string) : Referring URL the visitor is currently reviewing
        Raises:
            e: Exception
        Returns:
            string : search keywords
        """

        search_results = re.search(r'(search\?p\=(\w+\+\w+)|search\?q\=(\w+)|&q\=(\w+))', referrer_string)
        str = ''
        if search_results:
            for item in search_results.groups():
                if item is not None and '=' not in item:
                    str = str + item
            return str

    def process_files(self):

        """ Calculate revenue and top performing search keywords
        The analysis file is saved in tab seperated format on s3 location.
        """

        df = pd.read_csv(self.resp['Body'], sep='\t')
        # df = pd.read_csv("./data.tsv",delimiter= '\t')

        # extract revenue
        df['revenue'] = pd.to_numeric(df.product_list.dropna().apply(self._extract_revenue))
        # lambda - df['revenue'] = pd.to_numeric(df.product_list.dropna().apply(lambda x:x.split(';')[3]))

        # extract domain
        # df['search_domain'] = df.referrer.str.extract('(google|yahoo|bing).com')
        df['search_domain'] = df.referrer.str.extract('(\w+\.com)').replace('esshopzilla.com', np.NaN)

        # extract search_keywords
        df['search_keywords'] = df.referrer.dropna().apply(self._extract_searchwords)

        # print(df.head())

        # prepare the final output
        df_search_domain = df[['ip', 'search_domain']].dropna()
        df_search_keywords = df[['ip', 'search_keywords']].dropna()
        df_revenue = df[['ip', 'revenue']].dropna()

        # combined keywords and domain
        df_domain_keywords = df_search_domain.merge(df_search_keywords, how='inner', on='ip')

        # combined domain, keywords, revenue
        df_output = df_domain_keywords.merge(df_revenue, how='inner', on='ip').drop(columns='ip')

        file_name = datetime.now().strftime("%Y-%m-%d") + "_SearchKeywordPerformance.tab"

        print(df_output.columns)
        print(df_output.head())

        csv_buffer = StringIO()

        df_output.to_csv(csv_buffer, header=True, sep='\t', index=False)

        response = self.s3_client.put_object(Body=csv_buffer.getvalue(),
                                             Bucket=destination_bucket,
                                             Key=self.file_name)

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            logger.info(f"Successful S3 put_object response. Status - {status}")
        else:
            logger.info(f"Unsuccessful S3 put_object response. Status - {status}")


def lambda_handler(event, context):
    # TODO implement
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    file_path = f"s3://{bucket}/{key}"
    logger.info(f"file receieved .. {file_path}")
    resp = s3_client.get_object(Bucket=bucket, Key=key)
    revenue_run = HitsAnalyze(resp)
    # revenue_run = RevenueCalculation('./data.tsv')
    revenue_run.process_files()

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


if __name__ == '__main':
    test_event = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-east-1',
                               'eventTime': '2022-05-18T17:27:37.076Z', 'eventName': 'ObjectCreated:Copy',
                               'userIdentity': {'principalId': 'AWS:AIDA36OPGSI7UGENRUX4N'},
                               'requestParameters': {'sourceIPAddress': '135.84.42.99'},
                               'responseElements': {'x-amz-request-id': 'JNBGWPGCZATRAVRK',
                                                    'x-amz-id-2': 'CWSuVKxAG9ZMsQvjjHqmQyBHZH3KyZTdI/2erso4Jkwf1stM/oHBHsZnlYnkLCz4ClEnyrl8+HynvzAv1xpwkryXBB5fkQfU'},
                               's3': {'s3SchemaVersion': '1.0',
                                      'configurationId': 'c1d45746-b6e9-4ae0-ae27-ba5e156f52d9',
                                      'bucket': {'name': 'n-adobe-test',
                                                 'ownerIdentity': {'principalId': 'A1MURXB181E0ZI'},
                                                 'arn': 'arn:aws:s3:::n-adobe-test'},
                                      'object': {'key': 'hits-data/20220518/data.tsv', 'size': 6259,
                                                 'eTag': 'c5cf3ac1e540a3a98685b2787cc27059',
                                                 'sequencer': '0062852C8902C1CB43'}}}]}
    lambda_handler(test_event, context=None)
