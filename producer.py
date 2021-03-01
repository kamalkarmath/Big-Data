from kafka import KafkaProducer
import json
import requests
from json import dumps


KAFKA_TOPIC_NAME_CONS = "stockexchange"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS, value_serializer=lambda x: dumps(x).encode('utf-8'))

# Starting the rest api using the rapid api, python requests
    url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/auto-complete"

    querystring = {"q": "tesla", "region": "US"}

    headers = {
        'x-rapidapi-key': "dca6bf67aemsh5b33624e9913bbap13d573jsnca15665098ee",
        'x-rapidapi-host': "apidojo-yahoo-finance-v1.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

    print(response.text)


    print(json.dumps(response.json(), sort_keys=True, indent=4))
    kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, response.json())
    kafka_producer_obj.flush()