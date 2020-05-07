import socket
from concurrent.futures import ThreadPoolExecutor

import click
import requests

from walrus import Database

from polyd_events import consumer, producer
from polyd_events import communities as polyd_communities

from . import transfer, logging


@click.command()
@click.option('--community', '-c', multiple=True, type=click.Choice(list(polyd_communities)+['all']), default=['all'])
@click.option('--redis', '-h', type=click.STRING, envvar='POLYDMON_REDIS', default='127.0.0.1',
              help='redis hostname')
@click.option('--consumer-name', type=click.STRING, envvar='POLYDMON_CONSUMER_NAME', default=socket.gethostname(),
              help='consumer name')
@click.option('--access-key', type=click.STRING, envvar='PTS3_ACCESS_KEY', default='',
              help='S3 access key')
@click.option('--secret-key', type=click.STRING, envvar='PTS3_SECRET_KEY', default='',
              help='S3 secret key')
@click.option('--bucket', type=click.STRING, envvar='PTS3_BUCKET', default='',
              help='S3 bucket')
@click.option('--endpoint', type=click.STRING, envvar='PTS3_ENDPOINT', default='https://sfo2.digitaloceanspaces.com',
              help='S3 bucket')
@click.option('--region', type=click.STRING, envvar='PTS3_REGION', default='sfo2',
              help='S3 bucket')
@click.option('--expires', type=click.INT, envvar='PTS3_EXPIRES', default=60,
              help='file expiry in minutes')
@click.option('--psd-key', type=click.STRING, envvar='PTS3_PSD_KEY', default='',
              help='PSD api key')
@click.option('--quiet', '-q', is_flag=True, default=False)
def polyd_to_s3(community, redis, consumer_name, access_key, secret_key, bucket, endpoint, region, expires, psd_key, quiet):
    session = requests.Session()
    session.headers.update({'Authorization': psd_key})
    db = Database(redis)
    communities = community if 'all' not in community else polyd_communities

    streams = [f'polyd-{c}-bounty' for c in communities]

    c = consumer.EventConsumer(streams, 'polyd_to_s3', consumer_name, db)

    logger = logging.get_logger()

    if quiet:
        import logging as l
        logger.setLevel(l.WARN)

    # for now, we don't send these to 'all' which is really the websocket events
    producers = {c: producer.EventProducer(f'polyd-{c}-downloaded', db) for c in communities}

    with ThreadPoolExecutor() as executor:
        for event in c.iter_events():
            logger.info('Processing: %s', event)
            # only process FILE artifacts
            if event.artifact_type != 'FILE':
                continue
            client = transfer.get_client(access_key, secret_key, endpoint, region)
            key = event.uri
            executor.submit(transfer.event_to_s3, event, bucket, key,
                            client, producers[event.community], session, expires=expires)
