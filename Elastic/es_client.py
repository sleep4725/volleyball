import elasticsearch.exceptions
import yaml
import os
import socket

from elasticsearch import Elasticsearch
from elasticsearch.client.cluster import ClusterClient
##
#
## ==========================
class ElasticClusterError(Exception):
    def __init__(self):
        super().__init__('cluster에 문제가 있습니다.')

class ElasticClient:

    def __init__(self, es_config: str):
        self.es_client = ElasticClient.ret_es_client(es_config=es_config)
        self.es_indice = "volleball"

    @classmethod
    def ret_es_client(cls, es_config: str)-> Elasticsearch:

        result = os.path.exists(es_config)

        if result:
            with open(es_config, "r", encoding="utf-8") as fr:
                es_config = yaml.safe_load(fr)
                fr.close()
                es_hosts = [f"{es_config['esProtocol']}://{u}" for u in es_config["esHosts"]]

                try:

                    es = Elasticsearch(es_hosts,
                                       sniff_on_start=True,
                                       sniff_on_connection_fail=True,
                                       sniffer_timeout=60
                                       )
                except socket.timeout as err:
                    print(err)
                except elasticsearch.exceptions.TransportError as err:
                    print(err)
                else:
                    response = es.ping()
                    es_health_response = ClusterClient(client= es).health()
                    status = es_health_response.get("status")

                    if response and status in ["yellow", "green"]:
                        return es
                    else:
                        raise ElasticClusterError
        else:
            raise FileNotFoundError


# e = ElasticClient("../Config/es/es_config.yml")