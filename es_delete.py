"""
刪除index
"""

from elasticsearch import Elasticsearch

es = Elasticsearch("http://IP:9200/")

es.indices.delete(index='elk_test', ignore=[400, 404])