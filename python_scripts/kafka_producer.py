#
#
# consumes Kafka messages from a topic, 
# and uses OpenLineage HTTP client to
# call Marquez API

import openlineage
from confluent_kafka import Producer

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

run = openlineage.RunNamespace(
    name="my_job_run",
    facets={},
    inputs=[],
    outputs=[]
)

# openlineage.client.emit(openlineage.RunEvent(run))

openlineage_event = openlineage.client.emit(openlineage.RunEvent(run))
producer.produce(topic, key='optional_key', value=openlineage_event.serialize())

producer.flush()
producer.close()

