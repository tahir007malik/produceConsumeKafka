import threading
from confluent_kafka import DeserializingConsumer # we want our consumer to deserialize data before consuming
from confluent_kafka.schema_registry import SchemaRegistryClient # we need this client so that we can make connection to schema registry
from confluent_kafka.schema_registry.avro import AvroDeserializer # we need this to deserialize our data from avro format
from confluent_kafka.serialization import StringDeserializer # our key is of string while deserialzing we will deserialize our value as well as key

from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVER'),
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv('CLUSTER_API_KEY'),
    'sasl.password': os.getenv('CLUSTER_API_SECRET'),
    'group.id': 'group1', # here you can change the consumer groups
    'auto.offset.reset': 'earliest' # read strategy
}

# 'group.id': 'group1', # original group-id

# Create a Schema Registry client
"""
    we need object of schema registry client why?
    while subscribing the data from topic we need to fetch the schema from the schema registry so
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

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
    # 'enable.auto.commit': True,
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})

# Subscribe to the 'retail_data' topic
consumer.subscribe(['retail_data_test'])

#Continually read messages from Kafka
try:
    while True: # infinite loop
        msg = consumer.poll(1.0) # How many seconds to wait for message | polling means requesting for msg

        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue

        print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))

except KeyboardInterrupt:
    pass
finally:
    consumer.close()