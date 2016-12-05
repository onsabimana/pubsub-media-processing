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

import tempfile
import logging

from googleapiclient.discovery import build
from googleapiclient import http
from oauth2client.client import GoogleCredentials

from logger import Logger

DISCOVERY_URL = 'https://{api}.googleapis.com/$discovery/rest?version={apiVersion}'
TMP_SAVE_IMG = '/tmp/output.jpeg'

def create_api_client(which_api, version):
    """Returns a Cloud Logging service client for calling the API."""
    credentials = GoogleCredentials.get_application_default()
    return build(which_api, version, credentials=credentials, discoveryServiceUrl=DISCOVERY_URL)

def create_bq_client():
    """Returns a BigQuery service client for calling the API."""
    credentials = GoogleCredentials.get_application_default()
    return build('bigquery', 'v2', credentials=credentials)


class Mediator(object):

    def __init__(self, dropzone_bucket, filename, filetype, project_id, dataset_id, table_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

        self.dropzone_bucket = dropzone_bucket
        self.filename = filename
        self.filetype = filetype

        self.api_client = create_api_client('speech', 'v1beta1')
        self.bq_client = create_bq_client()

    def speech_to_text(self):
        """Builds a Speech API request for files in GCS and writes the response
           transcript and confidence to BigQuery for further analysis."""

        #[START speech_body]
        speech_body={
            'config': {
                'encoding': 'FLAC',
                'sampleRate': 16000,
                'languageCode': self.filename.split('_')[0]
            },
            'audio': {
                'uri': "gs://{0}/{1}".format(self.dropzone_bucket, self.filename)
            }
        }
        #[END speech_body]

        try:
            speech_request = self.api_client.speech().syncrecognize(body=speech_body)
            speech_response = speech_request.execute()
            chosen = speech_response['results'][0]['alternatives'][0]
            self.write_to_bq(chosen['transcript'], chosen['confidence'])
        except Exception, e:
            Logger.log_writer("Problem with file {0} with {1}".format(self.filename, str(e)))
            pass

    def write_to_bq(self, transcript, confidence):
        """Write to BigQuery"""
        Logger.log_writer("Writing - {} - to BigQuery".format(transcript))
        body = {
            "rows":[{
                "json": {
                    "transcript": transcript,
                    "confidence": confidence
                }
            }]
        }

        response = self.bq_client.tabledata().insertAll(
            projectId=self.project_id,
            datasetId=self.dataset_id,
            tableId=self.table_id,
            body=body
        ).execute()