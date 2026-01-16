from confluent_kafka import Consumer, KafkaError, KafkaException
from datetime import datetime, timedelta
import argparse 

def display_message(message):
  print("- '%s' %s %d %d" %
        (message.value(), message.topic(), message.offset(),
         message.timestamp()[1]))

parser = argparse.ArgumentParser()
parser.add_argument("topic_name", help="name of the topic to consume from")
parser.add_argument("secs", type=int, help="number of seconds reading from the topic")

args = parser.parse_args()

dt_start = datetime.now()
# Consumer setup
#
conf = {'bootstrap.servers': "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094",
        'auto.offset.reset': 'earliest',
        'group.id': 'python-group'
        }

consumer = Consumer(conf)

# Consumer subscription and message processing
#
try:
  consumer.subscribe([args.topic_name])

  while (dt_start + timedelta(seconds=args.secs))>datetime.now():
    message = consumer.poll(timeout=1.0)
    if message is None: continue

    if message.error():
      if message.error().code() == KafkaError._PARTITION_EOF:
        # End of partition event
        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                         (message.topic(), message.partition(), 
                          message.offset()))
      elif message.error():
        raise KafkaException(message.error())
    else:
      display_message(message)
finally:
  # Close down consumer to commit final offsets.
  consumer.close()
