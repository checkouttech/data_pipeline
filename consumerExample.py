
from kafka import KafkaConsumer
from kafka import TopicPartition

# To consumer messages from GROUP/TOPIC combination 
# To consume latest messages and auto-commit offsets 
consumer = KafkaConsumer('my-topic3',
                         group_id='my-group',
                         bootstrap_servers=['192.168.150.80:9092'])



# To consumer messages from a specific PARTITION 
# NOTE : will not get it for a group , so all messages of the partition will show 
# sniff specific partition but then cant assign to a group 
#consumer = KafkaConsumer(bootstrap_servers='192.168.150.80:9092')
#consumer.assign([TopicPartition('my-topic3', 1)])


# To show messages on the stdout
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("Topic= %s : Partition= %d : Offset= %d: key= %s value= %s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))

#
## to stream and write to a file 
#with open("test.txt", 'w', buffering=20*(1024**2)) as myfile:
#    for message in consumer:
#        # message value and key are raw bytes -- decode if necessary!
#        # e.g., for unicode: `message.value.decode('utf-8')`
#        print ("Topic= %s : Partition= %d : Offset= %d: key= %s value= %s" % (message.topic, message.partition,
#                                              message.offset, message.key,
#                                              message.value))
#    
#        myfile.write(str(message) + '\n')
#
#



'''

# consume earliest available messages, dont commit offsets
KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

# consume json messages
KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))

# consume msgpack
KafkaConsumer(value_deserializer=msgpack.unpackb)

# StopIteration if no message after 1sec
KafkaConsumer(consumer_timeout_ms=1000)

# Subscribe to a regex topic pattern
consumer = KafkaConsumer()
consumer.subscribe(pattern='^awesome.*')


# Use multiple consumers in parallel w/ 0.9 kafka brokers
# typically you would run each on a different server / process / CPU
consumer1 = KafkaConsumer('my-topic',
                          group_id='my-group',
                          bootstrap_servers='my.server.com')
consumer2 = KafkaConsumer('my-topic',
                          group_id='my-group',
                          bootstrap_servers='my.server.com')
'''
