#
#
# consumes Kafka messages from a topic, 
# and uses OpenLineage HTTP client to
# call Marquez API

from typing import List
import attr
from confluent_kafka import Producer
import hashlib

from openlineage.client.run import (
    RunEvent,
    RunState,
    Run,
    Job,
    Dataset,
    OutputDataset,
    InputDataset,
    JobEvent
)
from openlineage.client.client import OpenLineageClient, OpenLineageClientOptions
from openlineage.client.facet import (
    BaseFacet,
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

# PRODUCER = f"producer.py"
# namespace = "producer.py_test_namespace"
# dag_name = "user_trends"

# use sha512 or other?
DEFAULT_HASH_ALGORITHM = "sha256"

@attr.s
class DatasetIntegrityFacet(BaseFacet):
    hashValue: str = attr.ib()
    hashAlgorithm: str = attr.ib()
    _additional_skip_redact: List[str] = ['hashValue', 'hashAlgorithm']
    def __init__(self, hashValue, hashAlgorithm):
        super().__init__()
        self.hashValue = hashValue
        self.hashAlgorithm = hashAlgorithm

# generates job facet
def job(job_name, sql, location, namespace):
    facets = {"sql": SqlJobFacet(sql)}
    if location != None:
        facets.update(
            {"sourceCodeLocation": SourceCodeLocationJobFacet("git", location)}
        )
    return Job(namespace=namespace, name=job_name, facets=facets)


# generates run racet
def run(run_id, hour):
    return Run(
        runId=run_id,
        facets={
            # "nominalTime": NominalTimeRunFacet(
            #     nominalStartTime=f"2022-04-14T{twoDigits(hour)}:12:00Z"
            # )
        },
    )


# generates dataset
def dataset(name, namespace, schema=None):
    if schema == None:
        facets = {}
    else:
        facets = {"schema": schema}

    # TODO: how to retrieve data based on name/namespace to create hash?
    data = ""
    hash_value, hash_algorithm = getHash(data)
    
    facets.update(
        {'dataset_integrity': DatasetIntegrityFacet(hashValue=hash_value, hashAlgorithm=hash_algorithm)}
    )
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

def getHash(data, algorithm: str = DEFAULT_HASH_ALGORITHM):

    if algorithm not in hashlib.algorithms_available:
        algorithm = DEFAULT_HASH_ALGORITHM

    # DEFAULT_HASH_ALGORITHM (e.g. sha256)
    if algorithm not in hashlib.algorithms_guaranteed:
        algorithm = hashlib.algorithms_guaranteed

    hash_object = hashlib.new(name=algorithm)
    transformed_data = get_transformed_data(data)
    hash_object.update(transformed_data)
    hash_value = hash_object.hexdigest()
    return hash_value, algorithm 


def get_transformed_data(data) -> bytes:
    # TODO: to be specified
    return data