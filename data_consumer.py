# updated consumer
from pykafka import KafkaClient
from pykafka.simpleconsumer import OffsetType
import logging

logging.getLogger("pykafka.broker").setLevel('ERROR') #logging config

client = KafkaClient(hosts="localhost:9092")#kafka consumer client


topic = client.topics["sfcalls"]
consumer_messages = topic.get_balanced_consumer(
    consumer_group=b'pytkafka-test-2',
    auto_commit_enable=False,
    zookeeper_connect='localhost:2181'
)
for m in consumer_messages:
    if m is not None:
        print( m.value.decode('utf-8') )
