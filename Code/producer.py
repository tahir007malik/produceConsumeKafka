import datetime
import threading
from decimal import *
from time import sleep
from uuid import uuid4, UUID
import time

from confluent_kafka import SerializingProducer # we want our producer to serialize data then publish. [kafka has normal producer too]
from confluent_kafka.schema_registry import SchemaRegistryClient # we need this client so that we can make connection to schema registry
from confluent_kafka.schema_registry.avro import AvroSerializer # we need this to serialize our data in avro format
from confluent_kafka.serialization import StringSerializer # our key is of string while serialzing we will serialize our value as well as key
import pandas as pd
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

# callback function for our message publishing status
def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVER'),
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv('CLUSTER_API_KEY'),
    'sasl.password': os.getenv('CLUSTER_API_SECRET')
}

# Create a Schema Registry client
"""
    we need object of schema registry client why?
    while publishing the data from the producer we need to fetch the schema from the schema registry so
    we need schema registry client object here
"""
schema_registry_client = SchemaRegistryClient({
    # Stream Governance API: Endpoint here
    'url': os.getenv('STREAM_GOVERNANCE_ENDPOINT'),
    'basic.auth.user.info': '{}:{}'.format(os.getenv('STREAM_GOVERNANCE_KEY'), os.getenv('STREAM_GOVERNANCE_SECRET'))
})

# Fetch the latest Avro schema for the value
subject_name = 'retail_data_test-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Serializer for the value
# key_serializer = AvroSerializer(schema_registry_client=schema_registry_client, schema_str='{"type": "string"}')
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})


# Load the CSV data into a pandas DataFrame
df = pd.read_csv('/mnt/c/Users/tahir/Desktop/Development/testing/Confluent/Dataset/retail_data.csv')
df = df.fillna('null')
# print(df.head(20))

# Iterate over DataFrame rows and produce to Kafka
for index, row in df.iterrows():
    # Create a dictionary from the row values
    value = row.to_dict()
    # print(value)
    # Produce to Kafka
    producer.produce(topic='retail_data_test', key=str(index), value=value, on_delivery=delivery_report)
    producer.flush() # clearing message buffer for new messages
    time.sleep(1)
    #break

print("All Data successfully published to Kafka")   