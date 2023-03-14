#!/usr/bin/env python

import csv
import json
from typing import List, Dict
import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from ride_green import Ride
from settings import INPUT_DATA_PATH_green, KAFKA_TOPIC_green

def delivery_callback(err, msg):
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


class JsonProducer(Producer):
    def __init__(self, props: Dict):
        self.producer = Producer(**props)

    @staticmethod
    def read_records(resource_path: str):
        records = []
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header row
            for row in reader:
                records.append(Ride(arr=row))

  
        return records

    def delivery_callback(self,err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


    def publish_rides(self, topic: str, messages: List[Ride]):
        #print(messages[0])
        for ride in messages:
            #print(ride.Ride)
            #print(ride.DOLocationID)
            print(topic)

            key_ride=int(ride.DOLocationID)
            key_ride=str(key_ride).encode()
            value_ride=json.dumps(ride.__dict__, default=str).encode('utf-8')
            #print(key,value)

            self.producer.produce(topic=topic, key=key_ride, value=value_ride,callback=self.delivery_callback)
            self.producer.poll(100000)
            self.producer.flush()
            # print(record)




if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = JsonProducer(props=config)
    rides = producer.read_records(resource_path=INPUT_DATA_PATH_green)
    #print(rides)
    producer.publish_rides(topic=KAFKA_TOPIC_green, messages=rides)


