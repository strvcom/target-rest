#!/usr/bin/env python3

import argparse
import io
import os
import sys
import json
import threading
import http.client
import urllib
import requests
from datetime import datetime
from decimal import Decimal
import decimal
import collections
import math

import pkg_resources
from jsonschema.validators import Draft4Validator
import singer

REQUIRED_CONFIG_KEYS = ["api_url"]

logger = singer.get_logger()

def float_to_decimal(value):
    '''
    Walk the given data structure and turn all instances of float 
    into double.
    '''
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: float_to_decimal(v) for k, v in value.items()}
    return value


def numeric_schema_with_precision(schema):
    if 'type' not in schema:
        return False
    if isinstance(schema['type'], list):
        if 'number' not in schema['type']:
            return False
    elif schema['type'] != 'number':
        return False
    if 'multipleOf' in schema:
        return True
    return 'minimum' in schema or 'maximum' in schema


def get_precision(key, schema):
    v = abs(Decimal(schema.get(key, 1))).log10()
    if v < 0:
        return round(math.floor(v))
    return round(math.ceil(v))


def walk_schema_for_numeric_precision(schema):
    if isinstance(schema, list):
        for v in schema:
            walk_schema_for_numeric_precision(v)
    elif isinstance(schema, dict):
        if numeric_schema_with_precision(schema):
            scale = -1 * get_precision('multipleOf', schema)
            digits = max(get_precision('minimum', schema), get_precision('maximum', schema))
            precision = digits + scale
            if decimal.getcontext().prec < precision:
                logger.debug('Setting decimal precision to {}'.format(precision))
                decimal.getcontext().prec = precision
        else:
            for v in schema.values():
                walk_schema_for_numeric_precision(v)


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)

def send_data(data, url):
    """
    Send json data to REST endpoint at url
    :param data: JSON encoded data to be sent
    :param url: URL of endpoint where to send the data
    """
    # TODO: Handle authentification
    # TODO: Handle bad URL and issues with REST server
    r = requests.post(url, json=data)

    # If the response is not OK rise exception
    if not r.ok:
        raise Exception(f'REST API on {url} returned status code {r.status_code}')

    # TODO: r.ok is True for status_code < 400. 
    # Think about status_codes < 400 that can couse problems like `204 - No content` and handle them
    # https://en.wikipedia.org/wiki/List_of_HTTP_status_codes

def get_batch_size(config):
    """
    Returns batch size.
    If there is no batch size in config or if it is set to `null` or
    if it is set to invalid value (0, negative value, float, string, ...) it will set 
    batch size to 1 otherwise it will return the value set in configuration.
    :param config: Configuration dictionary
    :return: Valid batch size
    """
    batch_size = config.get('batch_size', None)
    if not isinstance(batch_size, int):
        batch_size = 1
    if batch_size < 1:
        batch_size = 1
    return batch_size


def persist_lines(config, lines):
    """
    Sends data to server line by line or in batches if `bat_size` in configuration is set.
    :param config: Target configuration
    :param lines: Lines with JSON data from tap
    :return: last state
    """
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}

    batch_size = get_batch_size(config)

    data = []
    
    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    # Loop over lines from stdin
    for line in lines:
        # Try to parse json data
        try:
            message = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise
        
        # Handle single record in message
        if  isinstance(message, singer.RecordMessage):
            stream = message.stream

            if stream not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(json_object['stream']))

            # Get schema for this record's stream
            schema = float_to_decimal(schemas[stream])

            # Get the record from message
            record = message.record

            # Validate record
            validators[stream].validate(float_to_decimal(record))

            # Send data to REST server
            data.append(record)
            if len(data) >= batch_size:
                send_data(data, config['api_url'])
                data = []

            state = None

        elif isinstance(message, singer.StateMessage):
            state = message.value
            logger.debug(f'Setting state to {state}')

        elif isinstance(message, singer.SchemaMessage):
            stream = message.stream
            schemas[stream] = message.schema
            schema = float_to_decimal(schemas[stream])
            
            walk_schema_for_numeric_precision(schema)

            validators[stream] = Draft4Validator(message.schema)
            key_properties[stream] = message.key_properties

        elif isinstance(message, singer.ActivateVersionMessage):
            # This is a signal to the Target that it should delete all previously
            # seen data and replace it with all the RECORDs it has seen where the
            # record's version matches this version number.
            
            # TODO: To implement this could be tricky. We would need specific enpoint 
            # that would delete all
            
            logger.warning('Ignoring ActivateVersionMessage')
        else:
            raise Exception(f'Unknown message type {message.type} in message {message}')
    
    # Send last batch that is smaller then batch_size (for cases when data_size % batch_size != 0)
    # TODO: This seem a little slopy, maybe better would be cover this directly in loop over the lines
    if len(data) > 0:
        send_data(data, config['api_url'])
    
    return state


def send_usage_stats():
    """
    Send anonymous usage data to singer.io
    """
    try:
        version = pkg_resources.get_distribution('target-rest-api').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-rest-api',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        response = conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')


def main():
    """
    Main function that parse configuration and start sending data to REST server
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        config = {}

    if not config.get('disable_collection', False):
        logger.info('Sending version information to singer.io. ' +
                    'To disable sending anonymous usage data, set ' +
                    'the config parameter "disable_collection" to true')
        threading.Thread(target=send_usage_stats).start()

    singer.utils.check_config(config, REQUIRED_CONFIG_KEYS)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(config, input)
        
    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
