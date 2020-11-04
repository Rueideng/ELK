
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import datetime
import time
import json
import sys
import requests
from IPython.display import clear_output, display
url = 'http://IP:9200'
data = requests.get(url)
print(data)

es = Elasticsearch('IP:9200')
print(es)


consumer = KafkaConsumer(
            bootstrap_servers=["IP:9092"],
            value_deserializer=lambda m: json.loads(m.decode('ascii')),
            auto_offset_reset="earliest",
            enable_auto_commit=True
            )

# # 步驟4. 讓Consumer向Kafka集群訂閱指定的topic
consumer.subscribe(topics="tryto")
try:
    print("Now listening for incoming messages ...")
    for record in consumer:
        # print(record)
        # clear_output(wait=True)
        msgKey = record.key
        msgValue = record.value
        mappings = {'whiskey_name': msgValue['WHISKEY_NAME'], 'user_name': msgValue['USER_NAME'], 'text': msgValue['TEXT'], 'language': msgValue['LANGUAGE'],
                    'score': msgValue['SCORE'], 'timestamp': msgValue['TIMESTAMP'], 'abv': msgValue['ABV'],
                    'brand_country': msgValue['BRAND_COUNTRY'], 'official_content': msgValue['OFFICIAL_CONTENT'], 'whickey_type': msgValue['WHICKEY_TYPE'],
                    'year': msgValue['YEAR'], 'location': msgValue["LOCATION"]}
        res = es.index(index='whiskey_kafka_test01', doc_type='_doc', body=mappings)
        print(res['result'])
except:
        # 錯誤處理
        e_type, e_value, e_traceback = sys.exc_info()
        print("type ==> %s" % (e_type))
        print("value ==> %s" % (e_value))
        print("traceback ==> file name: %s" % (e_traceback.tb_frame.f_code.co_filename))
        print("traceback ==> line no: %s" % (e_traceback.tb_lineno))
        print("traceback ==> function name: %s" % (e_traceback.tb_frame.f_code.co_name))
finally:
        consumer.close()







