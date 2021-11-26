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

LOG_LEVEL = os.getenv("LOG_LEVEL", "WARN")
MQTT_ADDRESS = os.getenv("MQTT_ADDRESS", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC = os.getenv("MQTT_TOPIC", "#")
PREFIX = os.getenv("GRPHITE_PREFIX", "mqtt_")
IGNORED_TOPICS = os.getenv("MQTT_IGNORED_TOPICS", "none").split(",")
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "60"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

INFLUXDB_ADDRESS = os.getenv('INFLUXDB_ADDRESS', '127.0.0.1')
INFLUXDB_PORT = int(os.getenv('INFLUX_PORT', "8086"))
INFLUXDB_USER = os.getenv("INFLUXDB_USERNAME")
INFLUXDB_PASSWORD = os.getenv("INFLUXDB_PASSWORD")
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE", 'mqtt')
INFLUXDB_TABEL = os.getenv("INFLUXDB_DATABASE", 'reading')

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
                            'fields': payload
                                    }],
                        'measurement': INFLUXDB_TABEL
                        }

 #   client = InfluxDBClient(host=influx_server,
 #                           port=influx_port)
    global influxdb_client
    
    success = influxdb_client.write(json_body,
                        # params isneeded, otherwise error 'database is required' happens
                        params={'db': INFLUXDB_DATABASE})

    if not success:
        print('error writing to database')

def _parse_dict(topic, payload):
    payloadlist = {}
    if  isinstance(payload, dict):
        for metric, value in payload.items():            
            if isinstance(value, dict):
                LOG.debug("parsing dict %s: %s", metric, value)
                data = _parse_dict(f"{topic}_{metric}", value)            
                for metric_, value_ in data.items():
                    try:
                        payloadlist[metric_] = _parse_metric(value_)
                    except:
                        pass
            else:
                payloadlist[topic + "_" + metric] = _parse_metric(value)
    else:
        try:
            payloadlist[topic]  = _parse_metric(payload)
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
        return None
    except UnicodeDecodeError:
        LOG.debug('encountered undecodable payload: "%s"', payload)
        return None

    if  isinstance(payload, dict):
        LOG.debug("dict")
        payloadlist = _parse_dict(topic, payload)

    # handle payload having single values and
    if not isinstance(payload, dict):
        LOG.debug("non dict")  
        value =  _parse_metric(payload)
        if value:
             return None     
        payloadlist = { topic: value }
    
    return payloadlist

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
    LOG.debug(f"Can't parse '{data}' to a number.")
    return None

def on_subscribe(mosq, userdata, mid, granted_qos):
    pass

def on_disconnect(mosq, userdata, rc):
    if rc == 0:
        logging.info("Clean disconnection")
    else:
        logging.info("Unexpected disconnect (rc %s); reconnecting in 5 seconds" % rc)
        time.sleep(5)

def _init_influxdb_database():
    global influxdb_client
    logging.debug('influxdb_client.get_list_database')
    databases = influxdb_client.get_list_database()
    logging.debug(databases)

    if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) == 0:
        logging.debug('Creating database %s' % INFLUXDB_DATABASE)
        influxdb_client.create_database(INFLUXDB_DATABASE)

    influxdb_client.create_retention_policy('10_days', '10d', 1, INFLUXDB_DATABASE, default=True)
    influxdb_client.create_retention_policy('30_days', '30d', 1, INFLUXDB_DATABASE, default=False)
    influxdb_client.create_retention_policy('infinite', 'INF', 1, INFLUXDB_DATABASE, default=False)
    
    influxdb_client.drop_continuous_query("mqtt_30_days","mqtt")
    select_clause = 'SELECT mean(*) INTO "mqtt.30_days" FROM "mqtt.10_days" GROUP BY time(5m)'
    influxdb_client.create_continuous_query('mqtt_30_days', select_clause, INFLUXDB_DATABASE, 'EVERY 10s FOR 5m')

    influxdb_client.drop_continuous_query("mqtt_infinite","mqtt")
    select_clause = 'SELECT mean(*) INTO "mqtt.infinite" FROM "mqtt.10_days" GROUP BY time(60m)'
    influxdb_client.create_continuous_query('mqtt_infinite', select_clause, INFLUXDB_DATABASE, 'EVERY 360s FOR 60m')

    influxdb_client.switch_database(INFLUXDB_DATABASE)
    logging.debug('Connected to database %s' % INFLUXDB_DATABASE)

def main():
    logging.info("Starting %s" % client_id)
    logging.info("INFO MODE")
    logging.debug("DEBUG MODE")
    
    global influxdb_client
    try:
        if INFLUXDB_USER and INFLUXDB_PASSWORD:
            logging.debug('InfluxDBClient 1')        
            influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, INFLUXDB_PORT, INFLUXDB_USER, INFLUXDB_PASSWORD, None)
        else:
            logging.debug('InfluxDBClient 2')        
            influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, INFLUXDB_PORT)
    except:
        sys.stderr.write("Can't create influx connection\n")
        sys.exit(1)
        
    logging.debug(influxdb_client)        
    logging.debug('Connecting to the database %s' % INFLUXDB_DATABASE)
    _init_influxdb_database()

    logging.debug('mqtt.Client()')
    global client
    client = mqtt.Client()
    logging.debug('stop_request')

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