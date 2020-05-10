import datetime
import logging

import boto3
import requests
from polyd_events import events
from . import logging

logger = logging.get_logger()


def get_client(access_key, secret_key, endpoint, region):
    session = boto3.session.Session(access_key, secret_key, region_name=region)
    return session.client('s3', endpoint_url=endpoint)


def event_to_s3(event, bucket, key, client, producer=None, session=None, expires=0):
    if not session:
        session = requests.Session()

    # eventually, would be good to use libpolyd here
    # for now, let's just do this manually
    # assumes one bounty / one file, which should always be the case now

    url = f'https://{event.community}.k.polyswarm.network/v1/artifacts/{event.uri}/0'
    logger.info('Downloading %s', url)
    try:
        with session.get(url, stream=True) as r:
            r.raise_for_status()
            if expires:
                expires = datetime.datetime.now() + datetime.timedelta(minutes=expires)
            else:
                expires = None
            client.upload_fileobj(r.raw, Bucket=bucket, Key=key, ExtraArgs={'Expires': expires, 'ContentEncoding': 'gzip'})
        logger.info('Downloaded %s.', url)
        if producer:
            new_url = f'{bucket}/{key}'
            file_event = events.FileDownloaded.from_path(event.community, new_url, event)
            producer.add_event(file_event)

        event.ack()
    except Exception as e:
        logger.exception('Error downloading %s: %s', url, e)     
