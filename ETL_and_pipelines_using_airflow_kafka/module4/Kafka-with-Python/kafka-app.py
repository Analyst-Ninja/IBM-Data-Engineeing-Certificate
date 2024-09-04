import threading, time

from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka import KafkaConsumer, KafkaProducer
import json

admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='first-app')

# topic_list = []

# new_topic = NewTopic(name="bankbranch", num_partitions= 2, replication_factor=1)
# topic_list.append(new_topic)

# admin_client.create_topics(new_topics=topic_list)

configs = admin_client.describe_configs(
    config_resources=[ConfigResource(ConfigResourceType.TOPIC, "bankbranch")])

# print(configs[0].resources[0][4][1])

producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))

producer.send("bankbranch", {'atmid':1, 'transid':100})
producer.send("bankbranch", {'atmid':2, 'transid':101})

consumer = KafkaConsumer('bankbranch',
                         auto_offset_reset='earliest')
for msg in consumer:
    print(msg.value.decode('utf-8'))