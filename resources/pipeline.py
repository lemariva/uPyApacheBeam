from __future__ import absolute_import

import argparse
import hashlib
import uuid
import logging
import re
import os
import json

import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.transforms import trigger
from apache_beam.io.external.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.trigger import AccumulationMode

from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore
from apache_beam.io.gcp.datastore.v1new.types import Entity
from apache_beam.io.gcp.datastore.v1new.types import Key
from apache_beam.io.gcp.datastore.v1new.types import Query

class LoggingDoFn(beam.DoFn):    
    def process(self, element):         
        logging.info(str(type(element)) + " :" + str(len(element)) + " [1]: " + str(element))
        return element

class EntityWrapper(object):
    """
    Create a Cloud Datastore entity from the given dictionary.
    Namespace and project are taken from the parent key.
    """
    def __init__(self, kind, parent_key):
        self._kind = kind
        self._parent_key = parent_key

    def make_entity(self, element):
        """Create entity from given string."""
        key = Key([self._kind, hashlib.sha1(json.dumps(element).encode('utf-8')).hexdigest()])
        entity = Entity(key)

        for key_dict, value in element.items():
            entity.set_properties({key_dict: value})

        return entity

def run(argv=None):
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)

    parser.add_argument(
      '--project', dest='project', default='core-iot-sensors', help='Project Name')
    group.add_argument(
        '--input_topic',
        help=(
            'Input PubSub topic of the form '
            '"projects/<PROJECT>/topics/<TOPIC>".'))
    group.add_argument(
        '--input_subs_esp32',
        help=(
            'Input PubSub subscription of the form '
            '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
    parser.add_argument(
      '--kind', dest='kind', default='esp32-iot', help='Firestore Collection')
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    kind = known_args.kind
    project = known_args.project

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_esp32 = beam.Pipeline(options=pipeline_options)

    ancestor_key = Key([kind, str(uuid.uuid4())], project=project)

    msg_esp32 = (
        pipeline_esp32 
        | beam.io.ReadFromPubSub(
            subscription=known_args.input_subscription).with_output_types(bytes)
        )

    save_data = (
                  msg_esp32 | 'Decode Data' >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
                      | 'To Entity' >> beam.Map(EntityWrapper(kind, ancestor_key).make_entity)
                      | 'Write to Datastore' >> WriteToDatastore(project)
                      | 'Debugging' >> beam.ParDo(LoggingDoFn())
                )

    result = pipeline_esp32.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()