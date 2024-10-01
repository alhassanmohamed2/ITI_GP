from confluent_kafka import Producer
import json
import time


def prod_function(data_list):
    conf = {'bootstrap.servers': "localhost:9092"}
    producer = Producer(**conf)

    for item in data_list:
        message = {
            'Beach Name': item['beach_name'],
            'Measurement Timestamp': item['measurement_timestamp'],
            'Water Temperature': item['water_temperature'],
            'Turbidity': item['turbidity'],
            'Transducer Depth': item['transducer_depth'],
            'Wave Height': item['wave_height'],
            'Wave Period': item['wave_period'],
            'Battery Life': item['battery_life'],
            'Measurement Timestamp Label': item['measurement_timestamp_label'],
            'Measurement ID': item['measurement_id']
        }
        message_encoded = json.dumps(message).encode('utf-8')
        producer.produce('my-topic', key=str(time.time()), value=(message_encoded))
        producer.poll(0)


    producer.flush()



