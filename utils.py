import docker
import pyspark
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession


def is_container_running(container_name):
    RUNNING = "running"

    docker_client = docker.from_env()
    try:
        container = docker_client.containers.get(container_name)
    except docker.errors.NotFound as exc:
        print(f"Check container name!\n{exc.explanation}")
    else:
        container_state = container.attrs["State"]
        return container_state["Status"] == RUNNING


def get_mymongo_container_ip():
    '''
    Get the exposed IP address of mymongo container and return it
    '''
    client = docker.DockerClient()
    is_container_running('mymongo')
    container = client.containers.get('mymongo')
    ip_add = container.attrs['NetworkSettings']['Networks']['mongodb-network'][
        'IPAddress']
    return ip_add


def create_spark_session(app_name, db_name, collection_name):
    '''
    create a SQL session between spark and mongodb collection,
    app_name: str
    db_name: str
    collection_name: str
    return SperkSession Object
    '''
    is_container_running('my_spark_master')
    is_container_running('my_spark_worker')
    mongo_uri = f'mongodb://localhost/{db_name}.{collection_name}'
    conf = pyspark.SparkConf().set(
        'spark.jars.packages',
        'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1'
    ).setMaster('local').setAppName(app_name).setAll(
        [('spark.driver.memory', '40g'), ('spark.executor.memory', '50g')])
    sc = SparkContext.getOrCreate(conf)
    ss = SparkSession.builder.getOrCreate()
    sqlc = SQLContext(sc, ss)
    online_retail = sqlc.read.format('com.mongodb.spark.sql.DefaultSource'
                                     ).option('uri', mongo_uri).load()
    online_retail.createOrReplaceTempView('online_retail')
    return sqlc.sparkSession
