conf = {'bootstrap.servers': 'localhost:9092'}

p = Producer(**conf)

def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %o\n' %
                         (msg.topic(), msg.partition(), msg.offset()))

try:
    p.produce(topic, 'hello pyconhk', callback=delivery_callback)
except BufferError as e:
    sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                     len(p))

# Wait until all messages have been delivered
sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
p.flush()
