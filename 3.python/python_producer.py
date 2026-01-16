from confluent_kafka import Producer
import argparse
import socket

def list_topics(producer):
  mTopics = producer.list_topics().topics

  if (len(mTopics) > 0):
    print("The local broker has the following topics available:")
    for topic_name in mTopics.keys():
      print("  - %s" % topic_name)
  else:
    print("The local broker doesn't have any topic")

def send_message(producer, key:str, value:str, topic:str):
  if value == None:
    print("ERROR: A message value has to be provided")
    return
  if topic == None:
    print("ERROR: A topic has to be provided")
    return

  producer.produce(topic,key=key, value=value,headers={'client' : 'python'})
  producer.flush()
  print("message produced in topic '%s'" % topic)

parser = argparse.ArgumentParser()
parser.add_argument("action", choices=['produce', 'list-topics'])
parser.add_argument("-v", "--value",
                    help="message value to send if action is 'send'")
parser.add_argument("-t", "--topic", 
                    help="topic where the message has to be sent if action is 'send'")
parser.add_argument("-k", "--key",
                    help="message key to send if action is 'send'")
args = parser.parse_args()

# Producer setup
conf = {'bootstrap.servers': "localhost:9092,localhost:9093,localhost:9094",
        'client.id': socket.gethostname(),
        'partitioner': 'murmur2_random' #https://www.confluent.io/blog/standardized-hashing-across-java-and-non-java-producers/
        }

producer = Producer(conf)

# Action dispatching
if args.action == "produce":
  send_message(producer,args.key, args.value, args.topic)
elif args.action == "list-topics":
  list_topics(producer)
