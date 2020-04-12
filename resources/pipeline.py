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
from apache_beam.transforms.trigger import AfterProcessingTime
from apache_beam.transforms.trigger import AfterCount
from apache_beam.transforms.trigger import Repeatedly
from apache_beam.transforms.trigger import AfterAny
from apache_beam.transforms.window import FixedWindows

from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore
from apache_beam.io.gcp.datastore.v1new.types import Entity
from apache_beam.io.gcp.datastore.v1new.types import Key
from apache_beam.io.gcp.datastore.v1new.types import Query

class LoggingDoFn(beam.DoFn):    
    def process(self, element, timestamp=beam.DoFn.TimestampParam):         
        logging.info(str(type(element)) + 
        " :" + str(len(element)) + 
        " [1]: " + str(element) + 
        " [dt]:" + str(timestamp.to_utc_datetime()))
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
        key = Key([self._kind, hashlib.sha1(json.dumps(element[1]).encode('utf-8')).hexdigest()])
        entity = Entity(key)

        for pipe in list(element[1]):
            records = element[1][pipe]
            if len(records) != 0:
                timestamp = records[0]["timestamp"]
                device_id = records[0]["device_id"]
                logging.info(device_id)     
                del records[0]["timestamp"]
                del records[0]["device_id"]
                entity.set_properties({"timestamp": timestamp})
                entity.set_properties({device_id: records[0]})
        
        return entity

def run(argv=None):
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
      '--project', dest='project', default='core-iot-sensors', help='Project Name')
    parser.add_argument(
      '--kind', dest='kind', default='iot-air', help='Firestore Collection')
    parser.add_argument(
        '--input_subs',
        dest='input_subs',
        help=(
            'Input PubSub subscription of the form '
            '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>,projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))

    known_args, pipeline_args = parser.parse_known_args(argv)
    kind = known_args.kind 
    project = known_args.project

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline = beam.Pipeline(options=pipeline_options)
    ancestor_key = Key([kind, str(uuid.uuid4())], project=project)

    messages = []
    for idx, subscription in enumerate(known_args.input_subs.split(',')):
        logging.info('START - subscribing to subs: %s' % (subscription))
        messages.append (
            pipeline 
            | 'Subscribe - ' + str(idx) >> 
                beam.io.ReadFromPubSub(subscription=subscription).with_output_types(bytes)
            | 'Decode Data - ' + str(idx) >> 
                beam.Map(lambda x: json.loads(x.decode('utf-8')))
            | 'Key/Value Data - ' + str(idx) >> 
                beam.Map(lambda row: (row["timestamp"], row))
            | 'With timestamp - ' + str(idx) >> beam.Map(
                lambda row: beam.window.TimestampedValue(row, row[0]))
            | 'Windowing - ' + str(idx) >> beam.WindowInto(
                        FixedWindows(1 * 5),
                        trigger=Repeatedly(
                             AfterAny(AfterCount(10), AfterProcessingTime(1 * 5))),
                        accumulation_mode=AccumulationMode.DISCARDING)
                        #allowed_lateness=60)
            )
    
    pipes = {}
    for idx, pipe in enumerate(messages):
        pipes["pipe_" + str(idx)] = pipe

    print(pipes)
    save_data = (
                pipes
                    | 'Group' >> beam.CoGroupByKey() 
                    #| 'Debugging A'>> beam.ParDo(LoggingDoFn())
                    | 'To Entity' >> beam.Map(EntityWrapper(kind, ancestor_key).make_entity)
                    | 'Write to Datastore' >> WriteToDatastore(project)
                    #| 'Debugging' >> beam.ParDo(LoggingDoFn())
                )
                
    result = pipeline.run()
    result.wait_until_finish()
    logging.info('END - Pipeline')

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()