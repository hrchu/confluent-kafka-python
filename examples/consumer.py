conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'default', 'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'}}

c = Consumer(conf, logger=logger)
c.subscribe(topics)

# Read messages from Kafka, print to stdout
try:
    while True:
        msg = c.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            # Error or event
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                 (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                # Error
                raise KafkaException(msg.error())
        else:
            # Proper message
            sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                             (msg.topic(), msg.partition(), msg.offset(),
                              str(msg.key())))
            print(msg.value())

except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')

# Close down consumer to commit final offsets.
c.close()
