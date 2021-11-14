from kafka import KafkaConsumer
from InitializeKafkaConsumer import InitializeKafkaConsumer
from InitializeElasticConnection import InitializeElasticConnection
import logging
import config

if __name__ == '__main__':
    kafka_consumer: InitializeKafkaConsumer = InitializeKafkaConsumer(config.topic_name, config.bootstrap_servers)
    elastic_connection: InitializeElasticConnection = InitializeElasticConnection(config.elastic_hosts)
    consumer: KafkaConsumer = kafka_consumer.get_kafka_connection()
    for message in consumer:
        if message is not None:
            try:
                elastic_connection.send_document_to_elastic(index_name=config.elastic_index_name,
                                                            document=message.value)
            except Exception as e:
                logging.error(e)
