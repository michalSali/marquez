#
#
# consumes Kafka messages from a topic, 
# and uses OpenLineage HTTP client to
# call Marquez API

from openlineage.client.run import (
    RunEvent,
    JobEvent,
    DatasetEvent,
    RunState
)

from confluent_kafka import Consumer, KafkaError
from marquez_client import MarquezClient

bootstrap_servers = 'localhost:9092'
group_id = 'test-consumer-group'
topic = 'test-topic'

consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'enable.auto.commit': False
    # Add any other necessary configurations
}

consumer = Consumer(consumer_config)

# Subscribe to a single topic
consumer.subscribe([topic])


# endpoint /api/v1/lineage is specified by default
url = "http://localhost:5000"

from openlineage.client.client import OpenLineageClient, OpenLineageClientOptions

client = OpenLineageClient(
    url=url,
    # optional api key in case marquez requires it. When running marquez in
    # your local environment, you usually do not need this.
    # options=OpenLineageClientOptions(api_key=api_key),
)

def process_kafka_event(msg) -> bool:
    # print(f"Received message: {message}")

    from openlineage.client.serde import Serde

    import json
    import openlineage

    message = msg.value().decode('utf-8')
    key = msg.key().decode('utf-8')

    # Deserialize the JSON object into an OpenLineage event
    event_dict = json.loads(message)
    # openlineage_event = openlineage.RunEvent.from_dict(openlineage_event_dict[message])
    # openlineage_event = openlineage.client.client.from_dict(openlineage_event_dict[message])

    # event_dict = Serde.to_dict(open)
    # print(event_dict)

    print(event_dict)
    # print(event_dict["type"])

    print(key)

    # use key, else use attributes from event_dict
    if (key in [ "RunEvent", "JobEvent", "DatasetEvent" ]):
        if key == "RunEvent":
            print("is RunEvent via key")
            eventType = str(event_dict['eventType']).upper()
            event_dict['eventType'] = RunState[eventType]
            openlineage_event = RunEvent(**event_dict)
        elif key == "JobEvent":
            print("is JobEvent via key")
            openlineage_event = JobEvent(**event_dict)
        else:
            print("is DatasetEvent via key")
            openlineage_event = DatasetEvent(**event_dict)
    else:
        if "run" in event_dict:
            print("is RunEvent via attr")
            eventType = str(event_dict['eventType']).upper()
            event_dict['eventType'] = RunState[eventType]
            openlineage_event = RunEvent(**event_dict)
        elif "job" in event_dict:
            print("is JobEvent via attr")
            openlineage_event = JobEvent(**event_dict)
        else:
            print("is DatasetEvent via attr")
            openlineage_event = DatasetEvent(**event_dict)

    # TODO: need to check if openlineage event has been successfully processed via Marquez,
    # i.e. if proper entities have been created in database
    # just need to check status code?
    # since client.emit returns None, might have to use client.transport.emit directly,
    # or copy the method but with `return client.transport.emit(...)` at the end
    #
    # since it's a POST request, we expect 201 - or maybe just 2XX?
    # and if it's 4XX or 5XX, it's false? what about 3XX?
    # - current marquez implementation has 201 for successful completion,
    #   200 for when provided JSON is not of LineageEvent type - not processed,
    #   other (error) status code otherwise (bad request = 400, or internal server error = 500)
    response = client.transport.emit(openlineage_event)

    # created
    if (response.status_code == 201):
        return True
    
    # skipped processing, serialized event is returned
    # TODO: try to create DatasetEvent, should return 200
    if (response.status_code == 200):
        print(f"unsupported event type by Marquez, event: {response.content}")
        return False
    
    # client error
    if (response.status_code == 400):
        print("bad request")
        return False
    
    # server error
    if (response.status_code == 500):
        print("internal server error")
        return False

    return False

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
        was_successfully_processed = process_kafka_event(msg)

        # > Commit the offset to indicate successful processing
        # > how to handle a situation where processing was successful
        #   (entity was created in Marquez DB), but offset wasn't commited,
        #   and therefore
        if was_successfully_processed:
            print("successfully processed message")
            consumer.commit(asynchronous=False)  # You can set asynchronous=True for asynchronous commits

except KeyboardInterrupt:
    print("interupt")
    raise Exception
    
finally:
    # Close the Kafka consumer when done
    consumer.close()


