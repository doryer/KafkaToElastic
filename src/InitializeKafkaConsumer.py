import json
import logging
from kafka import KafkaConsumer
from config import config


class InitializeKafkaConsumer:

    def __init__(self, topic_name: str, bootstrap_servers: list):
        self.topic_name = topic_name
        self.bootstrap_server = bootstrap_servers
        self.kafka_connection = self._initialize_kafka_consumer()

    def _initialize_kafka_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            self.topic_name,
            auto_offset_reset=config.kafka_consumer_offset,
            enable_auto_commit=True,
            group_id=config.kafka_consumer_group_id,
            value_deserializer=self._json_deserializer,
            bootstrap_servers=self.bootstrap_server)

    @staticmethod
    def _json_deserializer(message):
        try:
            return json.loads(message.decode(config.kafka_message_decode_mode))
        except json.decoder.JSONDecodeError:
            logging.exception('Unable to decode: %s', message)
            return None

    def get_kafka_connection(self):
        return self.kafka_connection
