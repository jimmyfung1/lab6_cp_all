import faust
import json
from datetime import datetime

app = faust.App(
    'faust_lab2',
    broker='kafka://localhost:9092',
    value_serializer='raw',
)

customer_landing_topic = app.topic('customer-landing')
customer_transform_topic = app.topic('customer-transform')

@app.agent(customer_landing_topic)
async def greet(customers):
    async for customer in customers:
        print("Received:", customer)

        customer_dict = json.loads(customer.decode("utf-8"))
        customer_dict['last_update'] = str(datetime.now())

        await customer_transform_topic.send(
            value=json.dumps(customer_dict).encode("utf-8")
        )
        