import logging

import boto3
import requests
from polyd_events import events
from . import logging

logger = logging.get_logger()


def get_client(access_key, secret_key, endpoint, region):
    session = boto3.session.Session(access_key, secret_key)
    return session.client('s3')


def event_to_s3(event, bucket, key, client, producer=None, session=None, expires=None):
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
            client.put_object(Bucket=bucket, Key=key, Body=r.raw)
        logger.info('Downloaded %s.', url)
        if producer:
            new_url = f'{bucket}/{key}'
            file_event = events.FileDownloaded(event.community, new_url, event)
            producer.add_event(file_event)

        event.ack()
    except Exception as e:
        logger.exception('Error downloading %s: %s', url, e)     
