"""
Copyright 2016 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from __future__ import division

import base64
import json
import time, datetime
import requests
import click

from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

from logger import Logger
from recurror import Recurror
from mediator import Mediator

METADATA_URL_PROJECT = "http://metadata/computeMetadata/v1/project/"
METADATA_URL_INSTANCE = "http://metadata/computeMetadata/v1/instance/"
METADTA_FLAVOR = {'Metadata-Flavor' : 'Google'}

# Get the metadata related to the instance using the metadata server
PROJECT_ID = requests.get(METADATA_URL_PROJECT + 'project-id', headers=METADTA_FLAVOR).text
INSTANCE_ID = requests.get(METADATA_URL_INSTANCE + 'id', headers=METADTA_FLAVOR).text
INSTANCE_NAME = requests.get(METADATA_URL_INSTANCE + 'hostname', headers=METADTA_FLAVOR).text
INSTANCE_ZONE_URL = requests.get(METADATA_URL_INSTANCE + 'zone', headers=METADTA_FLAVOR).text
INSTANCE_ZONE = INSTANCE_ZONE_URL.split('/')[0]

# Parameters to call with the script
@click.command()
@click.option('--toprocess', default=1,
              help='Number of medias to process on one instance at a time - Not implemented')
@click.option('--subscription', required=True, help='Name of the subscription to get new messages')
@click.option('--refresh', default=25, help='Acknowledge deadline refresh time')
@click.option('--dataset_id', default='media_processing', help='Name of the dataset where to save transcript')
@click.option('--table_id', default='speech', help='Name of the table where to save transcript')
def main(toprocess, subscription, refresh, dataset_id, table_id):
    sub = "projects/{0}/subscriptions/{1}".format(PROJECT_ID, subscription)

    r = Recurror(refresh - 10, postpone_ack)

    # pull() blocks until a message is received
    while True:
        #[START sub_pull]
        # Pull the data when available.
        resp = pubsub_client.projects().subscriptions().pull(
            subscription=sub,
            body={
                "maxMessages": toprocess
            }
        ).execute()
        #[END sub_pull]

        if resp:
            # Get the amount of media that one instance can processed
            # For this demo, we keep it to one per instance.
            m = resp.get('receivedMessages')[0]
            if m:
                message = m.get('message')
                ack_id = m.get('ackId')
                msg_string = base64.b64decode(message.get('data'))
                msg_data = json.loads(msg_string)
                bucket = msg_data["bucket"]
                filename = msg_data["name"]
                filetype = msg_data["type"]
                fn = filename.split('.')[0]

                # Start refreshing the acknowledge deadline.
                r.start(ack_ids=[ack_id], refresh=refresh, sub=sub)

                Logger.log_writer("{0} process starts".format(filename))
                start_process = datetime.datetime.now()

# <Your custom process>
                m = Mediator(bucket, filename, filetype, PROJECT_ID, dataset_id, table_id)
                m.speech_to_text()
# <End of your custom process>

                end_process = datetime.datetime.now()
                Logger.log_writer("{0} process stops".format(filename))

                
                #[START ack_msg]
                # Delete the message in the queue by acknowledging it.
                pubsub_client.projects().subscriptions().acknowledge(
                    subscription=sub,
                    body={
                        'ackIds': [ack_id]
                    }
                ).execute()
                #[END ack_msg]

                # Logs to see what's going on.
                Logger.log_writer(
                    "{media_url} processed by instance {instance_hostname} in {amount_time}"
                    .format(
                        media_url=msg_string,
                        instance_hostname=INSTANCE_NAME,
                        amount_time=str(end_process - start_process)
                    )
                )

                # Stop the ackDeadLine refresh until next message.
                r.stop()

def create_pubsub_client():
    """Returns a Cloud PubSub service client for calling the API."""
    credentials = GoogleCredentials.get_application_default()
    return build('pubsub', 'v1', credentials=credentials)

def create_gcs_client():
    """Returns a Cloud PubSub service client for calling the API."""
    credentials = GoogleCredentials.get_application_default()
    return build('storage', 'v1', credentials=credentials)

def postpone_ack(params):
    """Postpone the acknowledge deadline until the media is processed
    Will be paused once a message is processed until a new one arrives
    Args:
        ack_ids: List of the message ids in the queue
    Returns:
        None
    Raises:
        None
    """
    ack_ids = params['ack_ids']
    refresh = params['refresh']
    sub = params['sub']
    Logger.log_writer(','.join(ack_ids) + ' postponed')

    #[START postpone_ack]
    #Increment the ackDeadLine to make sure that file has time to be processed
    pubsub_client.projects().subscriptions().modifyAckDeadline(
        subscription=sub,
        body={
            'ackIds': ack_ids,
            'ackDeadlineSeconds': refresh
        }
    ).execute()
    #[END postpone_ack]

"""Create the API clients."""
pubsub_client = create_pubsub_client()
gcs_client = create_gcs_client()

"""Launch the loop to pull media to process."""
if __name__ == '__main__':
    main()
