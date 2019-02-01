import argparse
import json
import logging
import os
import random
import time
import uuid
import datetime

from kafka import KafkaProducer

CREDIT_CARD_NUMBER = [

        12345,
        73628,
        23455,
        54322,
        24555

]

TERMINAL = [
    { "id": "1", "countryCode": "US"},
    { "id": "2", "countryCode": "UK"},
    { "id": "3", "countryCode": "UK"},
    { "id": "4", "countryCode": "UK"}

]


def generate_event():
    ret = {
        'transactionNumber': str(random.randint(100000000, 9999999999)),
        'creditCardNumber': CREDIT_CARD_NUMBER[random.randint(0, 3)],
        'amount': str(random.randint(1000, 9999)),
        'timestamp': "1530279600000",
        'terminal': TERMINAL[random.randint(0, 3)]

    }
    return ret


def main(args):
    logging.info('brokers={}'.format(args.brokers))
    logging.info('topic={}'.format(args.topic))
    logging.info('rate={}'.format(args.rate))

    logging.info('creating kafka producer')
    producer = KafkaProducer(bootstrap_servers=args.brokers)

    logging.info('begin sending events')
    while True:
        producer.send(args.topic, json.dumps(generate_event()).encode(), "EVENT"+str(uuid.uuid4()))
        logging.info(generate_event())
        time.sleep(1.0 / int(args.rate))
    logging.info('end sending events')


def get_arg(env, default):
    return os.getenv(env) if os.getenv(env, '') is not '' else default


def parse_args(parser):
    args = parser.parse_args()
    args.brokers = get_arg('KAFKA_BROKERS', args.brokers)
    args.topic = get_arg('KAFKA_TOPIC', args.topic)
    args.rate = get_arg('RATE', args.rate)
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info('starting kafka-openshift-python emitter')
    parser = argparse.ArgumentParser(description='emit some stuff on kafka')
    parser.add_argument(
            '--brokers',
            help='The bootstrap servers, env variable KAFKA_BROKERS',
            default='localhost:9092')
    parser.add_argument(
            '--topic',
            help='Topic to publish to, env variable KAFKA_TOPIC',
            default='tenone-test')
    parser.add_argument(
            '--rate',
            type=int,
            help='Lines per second, env variable RATE',
            default=3)
    args = parse_args(parser)
    main(args)
    logging.info('exiting')
