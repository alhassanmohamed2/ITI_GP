from get_data_api import get_data
from kafka_producer import prod_function
import time
import subprocess

offset = 0
while True:
    prod_function(get_data(1, offset))
    offset += 1
    time.sleep(5)






