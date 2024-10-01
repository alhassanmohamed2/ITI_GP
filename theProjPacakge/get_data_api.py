from sodapy import Socrata

MyAppToken = 'XGJqZFuyRShKrdipreydb3Hu7'

client = Socrata('data.cityofchicago.org',
                 MyAppToken,
                 username="***************",
                 password="**************")


def get_data(num_of_buckets, size_of_data):
    results = []
    for i in range(num_of_buckets):
        test_re = client.get("qmqz-2xku", limit=size_of_data, offset= i * size_of_data, order='measurement_id')
        results = results + test_re
    return results
