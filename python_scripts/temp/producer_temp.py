#
#
# creates OpenLineage events, serializes them and pushes them to a Kafka topic
#

from openlineage.client.run import (
    RunEvent,
    RunState,
    Run,
    Job,
    Dataset,
    OutputDataset,
    InputDataset,
)
from openlineage.client.client import OpenLineageClient, OpenLineageClientOptions
from openlineage.client.facet import (
    SqlJobFacet,
    SchemaDatasetFacet,
    SchemaField,
    OutputStatisticsOutputDatasetFacet,
    SourceCodeLocationJobFacet,
    NominalTimeRunFacet,
    DataQualityMetricsInputDatasetFacet,
    ColumnMetric,
)
import uuid
from datetime import datetime, timezone, timedelta
import time
from random import random

PRODUCER = f"https://github.com/openlineage-user"
namespace = "python_client"
dag_name = "user_trends"

# generates job facet
def job(job_name, sql, location):
    facets = {"sql": SqlJobFacet(sql)}
    if location != None:
        facets.update(
            {"sourceCodeLocation": SourceCodeLocationJobFacet("git", location)}
        )
    return Job(namespace=namespace, name=job_name, facets=facets)


# geneartes run racet
def run(run_id, hour):
    return Run(
        runId=run_id,
        facets={
            "nominalTime": NominalTimeRunFacet(
                nominalStartTime=f"2022-04-14T{twoDigits(hour)}:12:00Z"
            )
        },
    )


# generates dataset
def dataset(name, schema=None, ns=namespace):
    if schema == None:
        facets = {}
    else:
        facets = {"schema": schema}
    return Dataset(namespace, name, facets)


# generates output dataset
def outputDataset(dataset, stats):
    output_facets = {"stats": stats, "outputStatistics": stats}
    return OutputDataset(dataset.namespace, dataset.name, dataset.facets, output_facets)


# generates input dataset
def inputDataset(dataset, dq):
    input_facets = {
        "dataQuality": dq,
    }
    return InputDataset(dataset.namespace, dataset.name, dataset.facets, input_facets)


def twoDigits(n):
    if n < 10:
        result = f"0{n}"
    elif n < 100:
        result = f"{n}"
    else:
        raise f"error: {n}"
    return result


now = datetime.now(timezone.utc)


# generates run Event
def runEvents(job_name, sql, inputs, outputs, hour, min, location, duration):
    run_id = str(uuid.uuid4())
    myjob = job(job_name, sql, location)
    myrun = run(run_id, hour)
    st = now + timedelta(hours=hour, minutes=min, seconds=20 + round(random() * 10))
    end = st + timedelta(minutes=duration, seconds=20 + round(random() * 10))
    started_at = st.isoformat()
    ended_at = end.isoformat()
    return (
        RunEvent(
            eventType=RunState.START,
            eventTime=started_at,
            run=myrun,
            job=myjob,
            producer=PRODUCER,
            inputs=inputs,
            outputs=outputs,
        ),
        RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=ended_at,
            run=myrun,
            job=myjob,
            producer=PRODUCER,
            inputs=inputs,
            outputs=outputs,
        ),
    )


# add run event to the events list
def addRunEvents(
    events, job_name, sql, inputs, outputs, hour, minutes, location=None, duration=2
):
    (start, complete) = runEvents(
        job_name, sql, inputs, outputs, hour, minutes, location, duration
    )
    events.append(start)
    events.append(complete)


events = []

# create dataset data
for i in range(0, 5):

    user_counts = dataset("tmp_demo.user_counts")
    user_history = dataset(
        "temp_demo.user_history",
        SchemaDatasetFacet(
            fields=[
                SchemaField(name="id", type="BIGINT", description="the user id"),
                SchemaField(
                    name="email_domain", type="VARCHAR", description="the user id"
                ),
                SchemaField(name="status", type="BIGINT", description="the user id"),
                SchemaField(
                    name="created_at",
                    type="DATETIME",
                    description="date and time of creation of the user",
                ),
                SchemaField(
                    name="updated_at",
                    type="DATETIME",
                    description="the last time this row was updated",
                ),
                SchemaField(
                    name="fetch_time_utc",
                    type="DATETIME",
                    description="the time the data was fetched",
                ),
                SchemaField(
                    name="load_filename",
                    type="VARCHAR",
                    description="the original file this data was ingested from",
                ),
                SchemaField(
                    name="load_filerow",
                    type="INT",
                    description="the row number in the original file",
                ),
                SchemaField(
                    name="load_timestamp",
                    type="DATETIME",
                    description="the time the data was ingested",
                ),
            ]
        ),
        "snowflake://",
    )

    create_user_counts_sql = """CREATE OR REPLACE TABLE TMP_DEMO.USER_COUNTS AS (
            SELECT DATE_TRUNC(DAY, created_at) date, COUNT(id) as user_count
            FROM TMP_DEMO.USER_HISTORY
            GROUP BY date
            )"""

    # location of the source code
    location = "https://github.com/some/airflow/dags/example/user_trends.py"

    # run simulating Airflow DAG with snowflake operator
    addRunEvents(
        events,
        dag_name + ".create_user_counts",
        create_user_counts_sql,
        [user_history],
        [user_counts],
        i,
        11,
        location,
    )




# ==================[ producer ]====================

from confluent_kafka.error import (KeySerializationError,
                    ValueSerializationError, KafkaError)

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

# Define a delivery report callback function
def on_delivery_logic(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))

        # check type of error

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
        # client.emit(event)

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

for event in events[:1]:
    from openlineage.client.serde import Serde

    produce_event_to_kafka(event)
    

producer.flush()
# producer.close()

