import os
import logging
from paho.mqtt import client as mqtt
from dotenv import load_dotenv

# Load environment variables from config.env
load_dotenv("config.env")

# Read configuration from environment variables
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "mqtt_subscriber")
MQTT_TOPIC = "devices/+/status"
LOG_FILE = "received_messages.log"
ERROR_LOG_FILE = "errors.log"

# Configure logging for received messages
message_logger = logging.getLogger("message_logger")
message_logger.setLevel(logging.INFO)
message_handler = logging.FileHandler(LOG_FILE)
message_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
message_logger.addHandler(message_handler)
# Disable propagation to avoid messages going to the root logger
message_logger.propagate = False

# Configure a separate logger for errors
error_logger = logging.getLogger("error_logger")
error_logger.setLevel(logging.ERROR)
error_handler = logging.FileHandler(ERROR_LOG_FILE)
error_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
error_logger.addHandler(error_handler)
# Disable propagation to avoid errors going to the root logger
error_logger.propagate = False

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to MQTT broker at {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}")
        client.subscribe(MQTT_TOPIC)
        print(f"Subscribed to topic: {MQTT_TOPIC}")
    else:
        error_message = f"Failed to connect, return code {rc}"
        print(error_message)
        error_logger.error(error_message)

def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload = msg.payload.decode('utf-8')
        log_message = f"Received message on topic '{topic}': {payload}"
        print(log_message)
        message_logger.info(log_message)
    except Exception as e:
        error_message = f"Error processing message: {str(e)}"
        print(error_message)
        error_logger.error(error_message)

def on_disconnect(client, userdata, rc):
    if rc != 0:
        error_message = "Unexpected disconnection from MQTT broker."
        print(error_message)
        error_logger.error(error_message)

def main():
    # Create an MQTT client instance with the specified protocol
    client = mqtt.Client(client_id=MQTT_CLIENT_ID, protocol=mqtt.MQTTv311)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    # Connect to the MQTT broker with error handling
    try:
        client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, keepalive=60)
        client.loop_forever()
    except Exception as e:
        error_message = f"Failed to connect or lost connection: {str(e)}"
        print(error_message)
        error_logger.error(error_message)

if __name__ == "__main__":
    main()

