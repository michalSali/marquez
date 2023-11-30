from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
from openlineage.client import OpenLineageClient
from datetime import datetime
from uuid import uuid4

# from confluent_kafka import Producer
import socket

# conf = {'bootstrap.servers': 'localhost:9092',
#         'client.id': socket.gethostname()}

# producer = Producer(conf)

# client = OpenLineageClient.from_environment()

client = OpenLineageClient(url="http://localhost:5000")

producer = "OpenLineage.io/website/blog"

inventory = Dataset(namespace="food_delivery", name="public.inventory")
menus = Dataset(namespace="food_delivery", name="public.menus_1")
orders = Dataset(namespace="food_delivery", name="public.orders_1")

job = Job(namespace="food_delivery", name="example.order_data")
run = Run(str(uuid4()))

client.emit(
    RunEvent(
        RunState.START,
        datetime.now().isoformat(),
        run, job, producer
    )
)

client.emit(
    RunEvent(
        RunState.COMPLETE,
        datetime.now().isoformat(),
        run, job, producer,
        inputs=[inventory],
        outputs=[menus, orders],
    )
)