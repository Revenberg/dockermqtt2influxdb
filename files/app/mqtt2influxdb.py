#!/usr/bin/env python3
"""mqtt2influxdb"""

import json
import logging
import re
import signal
import sys
import os
import time

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG")
MQTT_ADDRESS = os.getenv("MQTT_ADDRESS", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC = os.getenv("MQTT_TOPIC", "#")
PREFIX = os.getenv("GRPHITE_PREFIX", "mqtt_")
IGNORED_TOPICS = os.getenv("MQTT_IGNORED_TOPICS", "none").split(",")
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "60"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

INFLUXDB_ADDRESS = os.getenv('INFLUXDB_ADDRESS', '127.0.0.1')
INFLUXDB_PORT = int(os.getenv('INFLUX_PORT', 2003))
INFLUXDB_USER = os.getenv("INFLUXDB_USERNAME")
INFLUXDB_PASSWORD = os.getenv("INFLUXDB_PASSWORD")
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE", 'mqtt')

LOGFORMAT = '%(asctime)-15s %(message)s'

logging.basicConfig(level=LOG_LEVEL, format=LOGFORMAT)
LOG = logging.getLogger("mqtt2influxdb")

LOG.debug('MQTT_ADDRESS %s', MQTT_ADDRESS)
LOG.debug('INFLUXDB_ADDRESS %s', MQTT_ADDRESS)

STATE_VALUES = {
    "ON": 1,
    "OFF": 0,
    "TRUE": 1,
    "FALSE": 0,
}
client_id = "mqtt2influxdb_%d" % (os.getpid())

def cleanup(signum, frame):
    '''Disconnect cleanly on SIGTERM or SIGINT'''

    client.publish("/clients/" + client_id, "Offline")
    client.disconnect()
    logging.info("Disconnected from broker; exiting on signal %d", signum)
    sys.exit(signum)

def is_number(s):
    '''Test whether string contains a number (leading/traling white-space is ok)'''

    try:
        float(s)
        return True
    except ValueError:
        return False

def subscribe(client, userdata, flags, connection_result):  # pylint: disable=W0613
    """Subscribe to mqtt events (callback)."""
    LOG.info('listening to "%s"', TOPIC)
    client.subscribe(TOPIC)

def on_message(mosq, userdata, msg):
    logging.debug("%s", msg.topic)

    if (IGNORED_TOPICS[0] != ""):
        for iTopic in IGNORED_TOPICS:
            if iTopic in msg.topic:
                LOG.debug('Topic "%s" was ignored 1', msg.topic)
                return

        if msg.topic in IGNORED_TOPICS:
            LOG.debug('Topic "%s" was ignored 2', msg.topic)
            return

    payload = _parse_message(msg.topic, msg.payload)

    if not payload:
        return

    json_body = {'points': [{
                            'fields': {k: v for k, v in payload.items()}
                                    }],
                        'measurement': 'test'
                        }

 #   client = InfluxDBClient(host=influx_server,
 #                           port=influx_port)

    success = client.write(json_body,
                        # params isneeded, otherwise error 'database is required' happens
                        params={'db': INFLUXDB_DATABASE})

    if not success:
        print('error writing to database')

def _parse_dict(topic, payload):
    payloadlist = {}
    if  isinstance(payload, dict):
        for metric, value in payload.items():
            
            LOG.info( value )
            if isinstance(value, dict):
                LOG.debug("parsing dict %s: %s", metric, value)
                data = _parse_dict(f"{topic}_{metric}", value)            
                for metric_, value_ in data.items():
                    try:
                        payloadlist.add(metric_, _parse_metric(value_))
                    except:
                        pass
    else:
        try:
            payloadlist.add(topic, _parse_metric(payload))
        except:
            pass

    return payloadlist

def _parse_message(topic, payload):
    topic = topic.replace("/", ".").replace(" ", ".")
    # parse MQTT topic and payload
    try:
        payload = json.loads(payload)
    except json.JSONDecodeError:
        LOG.debug('failed to parse as JSON: "%s"', payload)
        return None, None
    except UnicodeDecodeError:
        LOG.debug('encountered undecodable payload: "%s"', payload)
        return None, None

    if  isinstance(payload, dict):
        payloadlist = _parse_dict(topic, payload)

    # handle payload having single values and
    if not isinstance(payload, dict):
        info = topic.split("/")
        payloadlist = {
            info[-1]: _parse_metric(payload)
        }

    return topic, payloadlist

def _parse_metric(data):
    """Attempt to parse the value and extract a number out of it.
    Note that `data` is untrusted input at this point.
    Raise ValueError is the data can't be parsed.
    """
    if isinstance(data, (int, float)):
        return data

    if isinstance(data, bytes):
        data = data.decode()

    if isinstance(data, str):
        data = data.upper()

        # Handling of switch data where their state is reported as ON/OFF
        if data in STATE_VALUES:
            return STATE_VALUES[data]

        # Last ditch effort, we got a string, let's try to cast it
        return float(data)

    # We were not able to extract anything, let's bubble it up.
    raise ValueError(f"Can't parse '{data}' to a number.")

def _parse_metrics(data, topic, prefix=""):
    """Attempt to parse a set of metrics.

    Note when `data` contains nested metrics this function will be called recursivley.
    """
    now = int(time.time())
    LOG.info( data )

    for metric, value in data.items():
        # when value is a dict recursivley call _parse_metrics to handle these messages
        LOG.info( value )
        
        try:
            metric_value = _parse_metric(value)
        except ValueError as err:
            LOG.debug("Failed to convert %s: %s", metric, err)
            continue
        
        LOG.debug("new value for %s: %s", metric, metric_value)

def on_subscribe(mosq, userdata, mid, granted_qos):
    pass

def on_disconnect(mosq, userdata, rc):
    if rc == 0:
        logging.info("Clean disconnection")
    else:
        logging.info("Unexpected disconnect (rc %s); reconnecting in 5 seconds" % rc)
        time.sleep(5)

def _init_influxdb_database(influxdb_client):
    databases = influxdb_client.get_list_database()
    if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) == 0:
        logging.debug('Creating database %s' % INFLUXDB_DATABASE)
        influxdb_client.create_database(INFLUXDB_DATABASE)
        influxdb_client.create_retention_policy('10_days', '10d', 1, INFLUXDB_DATABASE, default=True)
        influxdb_client.create_retention_policy('60_days', '60d', 1, INFLUXDB_DATABASE, default=False)
        influxdb_client.create_retention_policy('infinite', 'INF', 1, INFLUXDB_DATABASE, default=False)
    logging.debug('Switch database %s' % INFLUXDB_DATABASE)
    
    influxdb_client.switch_database(INFLUXDB_DATABASE)
    logging.debug('Connected to database %s' % INFLUXDB_DATABASE)

def main():
    logging.info("Starting %s" % client_id)
    logging.info("INFO MODE")
    logging.debug("DEBUG MODE")
    
    try:
        if INFLUXDB_USER and INFLUXDB_PASSWORD:
            influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, INFLUXDB_PORT, INFLUXDB_USER, INFLUXDB_PASSWORD, None)
        else:
            influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, INFLUXDB_PORT)
    except:
        sys.stderr.write("Can't create influx connection\n")
        sys.exit(1)
        
    logging.debug('Connecting to the database %s' % INFLUXDB_DATABASE)
    _init_influxdb_database(influxdb_client)

    global client
    client = mqtt.Client()

    def stop_request(signum, frame):
        """Stop handler for SIGTERM and SIGINT.
        Keyword arguments:
        signum -- signal number
        frame -- None or a frame object. Represents execution frames
        """
        LOG.warning("Stopping MQTT exporter")
        LOG.debug("SIGNAL: %s, FRAME: %s", signum, frame)
        client.disconnect()
        sys.exit(0)

    logging.debug('stop_request')
    
    signal.signal(signal.SIGTERM, stop_request)
    signal.signal(signal.SIGINT, stop_request)

    logging.debug('on_message')
    client.on_message = on_message
    logging.debug('subscribe')
    client.on_connect = subscribe    
#    client.on_disconnect = on_disconnect
#    client.on_subscribe = on_subscribe

#    client.will_set("clients/" + client_id, payload="Adios!", qos=0, retain=False)

    # start the connection and the loop
    logging.debug('MQTT_USERNAME')
    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    LOG.info("MQTT: %s %s %s", MQTT_ADDRESS, MQTT_PORT, MQTT_KEEPALIVE)
    client.connect(MQTT_ADDRESS, MQTT_PORT, MQTT_KEEPALIVE)
    client.loop_forever()

if __name__ == '__main__':
        main()