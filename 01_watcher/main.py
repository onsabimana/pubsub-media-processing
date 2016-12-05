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

import os
import json
import logging
import base64
import webapp2

from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

PROJECT_ID = os.getenv('APPLICATION_ID').split('~')[1]
PUBSUB_TOPIC = "mediap-topic" # YOUR PUBSUB MEDIA TOPIC NAME


def create_pubsub_client():
    """Returns a Cloud PubSub service client for calling the API."""
    credentials = GoogleCredentials.get_application_default()
    return build('pubsub', 'v1', credentials=credentials)


class MainPage(webapp2.RequestHandler):
    """Process notification events."""
    def get(self):
        logging.info("Get request to notification page.")
        self.response.write("Welcome to the notification app.")

    def post(self):
        """Process the notification event.

        This method is invoked when the notification channel is first created with
        a sync event, and then subsequently every time an object is added to the
        bucket, updated (both content and metadata) or removed. It records the
        notification message in the log.
        """

        #logging.debug(%s\n\n%s',
        #               '\n'.join(['%s: %s' % x for x in self.request.headers.iteritems()]),
        #               self.request.body)

        if 'X-Goog-Resource-State' in self.request.headers:
            resource_state = self.request.headers['X-Goog-Resource-State']

            if resource_state == 'sync':
                logging.info('Sync message received.')

            elif resource_state == 'exists':
                # TODO Check the file type is a media

                pubsub_client = create_pubsub_client()
                data = json.loads(self.request.body)

                logging.info(self.request)
                logging.info(self.request.headers)
                logging.info(data)

                #[START msg_format]
                # Publish message to topic
                message = json.dumps({
                    'selfLink': data['selfLink'],
                    'bucket'  : data['bucket'],
                    'name'    : data['name'],
                    'type'    : data['contentType']
                })
                logging.debug(message)
                #[END msg_format]

                #[START topic_publish]
                body = {
                    'messages': [
                        {'data': base64.b64encode(message)}
                    ]
                }
                resp = pubsub_client.projects().topics().publish(
                    topic='projects/%s/topics/%s' % (PROJECT_ID, PUBSUB_TOPIC),
                    body=body
                ).execute()
                #[END topic_publish]

            else:
                logging.debug("The action was not and upload")

        else:
            logging.info("Other post.")

logging.getLogger().setLevel(logging.DEBUG)
app = webapp2.WSGIApplication([('/media-processing-hook', MainPage)], debug=True)
