from elasticsearch import Elasticsearch


class InitializeElasticConnection:
    def __init__(self, elastic_hosts: list):
        self.elastic_hosts = elastic_hosts
        self.elastic_connection = self._initialize_elastic_connection()

    def _initialize_elastic_connection(self) -> Elasticsearch:
        return Elasticsearch(hosts=self.elastic_hosts)

    def get_elastic_connection(self) -> Elasticsearch:
        return self.elastic_connection

    def send_document_to_elastic(self, index_name: str, document: dict):
        self.elastic_connection.index(index=index_name, document=document)
