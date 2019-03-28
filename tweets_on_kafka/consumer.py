from kafka import KafkaConsumer


def start_consuming_message(topic, group_id, bootstrap_servers):
    consumer = KafkaConsumer(
        topic,
        group_id=group_id,
        bootstrap_servers=bootstrap_servers
    )

    for message in consumer:
        print(
            "%s:%d:%d: key=%s value=%s" % (
                message.topic,
                message.partition,
                message.offset, message.key,
                message.value.decode('utf8')
            )
        )


if __name__ == '__main__':
    start_consuming_message(
        bootstrap_servers=['192.168.0.12:9092'],
        group_id='consumer_blue',
        topic='test1'
    )
