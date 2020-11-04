from kafka import KafkaProducer
import csv
import datetime
import time
import random
import sys
import  re
import json

def value_serializer(m):
    return json.dumps(m).encode()


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj,datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return json.JSONEncoder.default(self,obj)




csvfile = open('./kibana_whiskey_all_1.csv', 'r',encoding='utf-8',errors='ignore')
reader = csv.reader(csvfile)
producer = KafkaProducer(
    # 指定Kafka集群伺服器
    bootstrap_servers=["IP:9092"],
    value_serializer = lambda m: json.dumps(m).encode('ascii'),
    # api_version=(2,6,0),

)
print(producer.config)  ##打印配置信息
try:
    for row in reader:
        if reader.line_num == 1:  # 去除第一列(欄位名稱)
            continue  # skip first row
        else:
            timestamp = datetime.datetime.strptime(row[9], "%Y-%m-%d %H:%M:%S")
            location = row[15]
            lat = float(re.sub(r'[\[\]]', '', location).split(', ')[1])
            lon = float(re.sub(r'[\[\]]', '', location).split(',')[0])

            mappings = {'whiskey_name': row[0], 'group': row[1], 'user_name': row[2], 'gender': row[3], 'age': row[4],
                        'age_group': row[5], 'text': row[6], 'language': row[7],
                        'score': row[8], 'timestamp':  row[9] , 'abv': row[10], 'brand_country': row[11],
                        'official_content': row[12], 'whickey_type': row[13], 'year': row[14],
                        'location': {'lat': lat, 'lon': lon}}
            # mappings = json.dumps(mappings,cls=DateEncoder)
            future = producer.send(topic="tryto", value=mappings)
            time.sleep(1)
            print(future)
            # print(producer.config)

except Exception as e:
    # 錯誤處理
    e_type, e_value, e_traceback = sys.exc_info()
    print("type ==> %s" % (e_type))
    print("value ==> %s" % (e_value))
    print("traceback ==> file name: %s" % (e_traceback.tb_frame.f_code.co_filename))
    print("traceback ==> line no: %s" % (e_traceback.tb_lineno))
    print("traceback ==> function name: %s" % (e_traceback.tb_frame.f_code.co_name))

