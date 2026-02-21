from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'customer-transform',
    group_id='lab-group',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest'
)

print("Final consumer started...")

with open('customer_output.json', 'w', encoding='utf-8') as f:
    for message in consumer:
        decoded_string = message.value.decode("utf-8")
        print("Message from kafka:", decoded_string)

        f.write(decoded_string + "\n")
        f.flush()