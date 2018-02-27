"""aws_sat_api.aws"""

import os

from boto3.session import Session as boto3_session

region = os.environ.get('AWS_REGION', 'us-east-1')


def get_object(bucket, key, request_pays=False):
    """AWS s3 get object content
    """
    session = boto3_session(region_name=region)
    s3 = session.client('s3')

    params = {'Bucket': bucket, 'Key': key}
    if request_pays:
        params['RequestPayer'] = 'requester'

    response = s3.get_object(**params)
    return response['Body'].read()
