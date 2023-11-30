#
#
# creates OpenLineage events, serializes them and pushes them to a Kafka topic
#

# ==================[ producer ]====================

import time
from confluent_kafka.error import (KeySerializationError,
                    ValueSerializationError, KafkaError)

from confluent_kafka import Producer
from openlineage.client.serde import Serde

bootstrap_servers = 'localhost:9092'
group_id = 'test-consumer-group'
topic = 'test-topic'



# Configure Kafka producer
producer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    # Add any other necessary configurations
}

producer = Producer(producer_config)

# Define a delivery report callback function
def on_delivery_logic(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))

        # check type of error ?

        # retry
        send_message(msg.topic(), msg.key(), msg.value())
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def send_message(topic, key, value):
    producer.produce(topic, key=key, value=value, on_delivery=on_delivery_logic)

def produce_event_to_kafka(event, eventType="RunEvent"):
    try:
        serialized_event = Serde.to_json(event)
        print("sending event")
        # print(event)
        # print(serialized_event)
        time.sleep(1)
        
        """
        Raises:
                BufferError: if the internal producer message queue is full.
                    (``queue.buffering.max.messages`` exceeded). If this happens
                    the application should call :py:func:`SerializingProducer.Poll`
                    and try again.

                KeySerializationError: If an error occurs during key serialization.

                ValueSerializationError: If an error occurs during value serialization.

                KafkaException: For all other errors
        """
        send_message(topic, key=eventType, value=serialized_event)
        
    except BufferError:
        pass
    except KeySerializationError:
        pass
    except ValueSerializationError:
        pass
    except KafkaError:
        pass

# need to create events based on executed job/run
def get_events():
    return []

events = get_events()

for event in events:
    produce_event_to_kafka(event)
    

producer.flush()
# producer.close()

