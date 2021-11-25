#!/usr/bin/env python3
"""MQTT2graphite"""

import json
import logging
import re
import signal
import sys
import os
import time
import socket

import paho.mqtt.client as mqtt

LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG")
MQTT_ADDRESS = os.getenv("MQTT_ADDRESS", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC = os.getenv("MQTT_TOPIC", "#")
PREFIX = os.getenv("GRPHITE_PREFIX", "mqtt_")
IGNORED_TOPICS = os.getenv("MQTT_IGNORED_TOPICS", "none").split(",")
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "60"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

CARBON_SERVER = os.getenv('CARBON_SERVER', '127.0.0.1')
CARBON_PORT = int(os.getenv('CARBON_PORT', 2003))

LOGFORMAT = '%(asctime)-15s %(message)s'

logging.basicConfig(level=LOG_LEVEL, format=LOGFORMAT)
LOG = logging.getLogger("mqtt2graphite")

LOG.debug('MQTT_ADDRESS %s', MQTT_ADDRESS)

STATE_VALUES = {
    "ON": 1,
    "OFF": 0,
    "TRUE": 1,
    "FALSE": 0,
}
client_id = "MQTT2Graphite_%d-%s" % (os.getpid(), socket.getfqdn())
lines = []

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

    topic, payload = _parse_message(msg.topic, msg.payload)

    if not topic or not payload:
        return

    global lines
    lines = []
    _parse_metrics(payload, topic)
    logging.debug(lines)

    message = '\n'.join(lines) + '\n'
    logging.debug("%s", message)

    sock.sendto(message.encode(), (CARBON_SERVER, CARBON_PORT))

def _parse_message(topic, payload):
    topic = topic.replace("/", "_").replace(" ", "_")
    # parse MQTT topic and payload
    try:
        payload = json.loads(payload)
    except json.JSONDecodeError:
        LOG.debug('failed to parse as JSON: "%s"', payload)
        return None, None
    except UnicodeDecodeError:
        LOG.debug('encountered undecodable payload: "%s"', payload)
        return None, None

    # handle payload having single values and
    if not isinstance(payload, dict):
        info = topic.split("/")
        payload = {
            info[-1]: payload
        }

    return topic, payload

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
    global lines

    LOG.info( data )

    for metric, value in data.items():
        # when value is a dict recursivley call _parse_metrics to handle these messages
        LOG.info( value )
        if isinstance(value, dict):
            LOG.debug("parsing dict %s: %s", metric, value)
            _parse_metrics(value, topic, f"{prefix}{metric}_")
            continue

        try:
            metric_value = _parse_metric(value)
        except ValueError as err:
            LOG.debug("Failed to convert %s: %s", metric, err)
            continue
        carbonkey = PREFIX + topic.replace('/', '.')
        lines.append("%s %f %d" % (carbonkey, metric_value, now))
        LOG.debug("new value for %s: %s", carbonkey, metric_value)

def on_subscribe(mosq, userdata, mid, granted_qos):
    pass

def on_disconnect(mosq, userdata, rc):
    if rc == 0:
        logging.info("Clean disconnection")
    else:
        logging.info("Unexpected disconnect (rc %s); reconnecting in 5 seconds" % rc)
        time.sleep(5)


def main():
    logging.info("Starting %s" % client_id)
    logging.info("INFO MODE")
    logging.debug("DEBUG MODE")

    global sock
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except:
        sys.stderr.write("Can't create UDP socket\n")
        sys.exit(1)

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

    signal.signal(signal.SIGTERM, stop_request)
    signal.signal(signal.SIGINT, stop_request)

    client.on_message = on_message
    client.on_connect = subscribe
#    client.on_disconnect = on_disconnect
#    client.on_subscribe = on_subscribe

#    client.will_set("clients/" + client_id, payload="Adios!", qos=0, retain=False)

    # start the connection and the loop
    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    LOG.info("MQTT: %s %s %s", MQTT_ADDRESS, MQTT_PORT, MQTT_KEEPALIVE)
    client.connect(MQTT_ADDRESS, MQTT_PORT, MQTT_KEEPALIVE)
    client.loop_forever()

if __name__ == '__main__':
        main()