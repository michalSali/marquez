#
#
# consumes Kafka messages from a topic, 
# and uses OpenLineage HTTP client to
# call Marquez API

from confluent_kafka import Consumer, KafkaError

bootstrap_servers = 'localhost:9092'
group_id = 'test-consumer-group'
topic = 'test-topic'

consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    # Add any other necessary configurations
}

consumer = Consumer(consumer_config)

# Subscribe to a single topic
consumer.subscribe([topic])


url = "localhost:5000"

from openlineage.client.client import OpenLineageClient, OpenLineageClientOptions

client = OpenLineageClient(
    url=url,
    # optional api key in case marquez requires it. When running marquez in
    # your local environment, you usually do not need this.
    # options=OpenLineageClientOptions(api_key=api_key),
)

def process_kafka_event(message):
    print(f"Received message: {message}")

    from openlineage.client.serde import Serde

    import json
    import openlineage
    # Deserialize the JSON object into an OpenLineage event
    openlineage_event_dict = json.loads(message)
    openlineage_event = openlineage.RunEvent.from_dict(openlineage_event_dict[message])
    openlineage_event = openlineage.client.client.from_dict(openlineage_event_dict[message])

    
    event = Serde.to_dict

    client.emit(openlineage_event)
    # Add your custom processing logic here

try:
    while True:
        msg = consumer.poll(timeout=1000)  # Adjust the timeout as needed

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event, not an error
                continue
            else:
                print(f"Error: {msg.error()}")
                break

        # Process the Kafka event
        process_kafka_event(msg.value())

except KeyboardInterrupt:
    pass
finally:
    # Close the Kafka consumer when done
    consumer.close()


