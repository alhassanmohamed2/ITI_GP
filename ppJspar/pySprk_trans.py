from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
from confluent_kafka import Consumer, KafkaError
import json
import time
from transformation import Transf_data
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType


def spark_b(json_data):  

   # Define the schema
    schema = StructType([
        StructField("beach_name", StringType(), True),
        StructField("measurement_timestamp", StringType(), True),
        StructField("water_temperature_celsius", FloatType(), True),
        StructField("turbidity_ntu", FloatType(), True),
        StructField("transducer_depth_meters", FloatType(), True),
        StructField("wave_height_meters", FloatType(), True),
        StructField("wave_period_seconds", IntegerType(), True),
        StructField("battery_life_voltage", FloatType(), True),
        StructField("measurement_id", StringType(), True),
        StructField("battery_status", StringType(), True)
    ])

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Simple Test") \
        .getOrCreate()
    rdd = spark.sparkContext.parallelize([json_data])
    # Create DataFrame from the list of dictionaries
    df = spark.createDataFrame(rdd, schema)
    df_dicts = df.rdd.map(lambda row: row.asDict()).collect()
    return df_dicts[0]


def els(id, data):
    es = Elasticsearch(
    ["http://localhost:9200"],
    verify_certs= False 
)
    data['num_order'] = id
    es.index(index="iot_data", id=id, document=data )

def check_els_id():
    es = Elasticsearch(
    ["http://localhost:9200"],
    verify_certs= False 
)
    response = es.search(
        index='iot_data',
        body={
            "sort": [
                {"num_order": {"order": "desc"}}
            ],
            "size": 1
        }
    )
    

    return int(response['hits']['hits'][0]['_id'])

def check_table_id(table_id):

    es = Elasticsearch(
    ["http://localhost:9200"],
    verify_certs= False 
    )
    search_query = {
    "query": {
        "match": {
            "measurement_id": f"{table_id}"
        }
    }
}

    response = es.search(index='iot_data', body=search_query)

    if response['hits']['total']['value'] > 0:
        return True
    else:
        return False


def kafka_cons():

    consumer_conf = {
        'bootstrap.servers': 'localhost:9092', 
        'group.id': 'my-consumer-group',       
        'auto.offset.reset': 'earliest'        
    }

    consumer = Consumer(consumer_conf)

    consumer.subscribe(['my-topic'])
    i = 0
    try:
        i = check_els_id()
    except Exception:
        i += 1
    try:
        while True:
            msg = consumer.poll(1.0)  
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:

                    print(f"Reached end of partition: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                else:
                    print(f"Error: {msg.error()}")
                continue
            message_value = msg.value().decode('utf-8')
            # try:
            #     if check_table_id(Transf_data(message_value)['measurement_id']) :
            #         continue
            # except Exception:
            #     pass
            i+=1
            els(i, spark_b(Transf_data(message_value)))
            

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
   
kafka_cons()





