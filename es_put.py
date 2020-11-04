"""
將座標經緯度轉乘geo_point格式
把資料上傳到kibana

"""
from elasticsearch import Elasticsearch
import csv
import datetime


#=================測試有無連線成功(出現 <Response [200]> 為成功)
# import requests
# url = 'http://IP:9200'
# data = requests.get(url)
# print(data)
# =================


es = Elasticsearch('http:IP//:9200')
#============================================= 座標圖需要在kibna console上輸入以下資訊 把座標經緯度轉萬成geo_point格式
# PUT /whiskey
# {
#    "mappings": {
#          "properties": {
#             "location": {
#                "type": "geo_point"
#             }
#          }
#       }
# }

#===========================================



def put_word_weight():
    n = 0
    with open('../text mining/final/whisky_weight.csv', newline='', encoding='utf8') as csvfile:
        # 讀取 CSV 檔案內容
        rows = csv.reader(csvfile)
        # 以迴圈輸出每一列
        for row in rows:
            if rows.line_num == 1:
                continue
            else:
                # 將資料寫入ES
                doc = {"woed": row[0], "weight": row[1], }
                # print(doc)
                res = es.index(index='whiskey_word_weight', doc_type='_doc', body=doc)
                print(res['result'])
                n += 1
                print(n)



def put_comment_score():
    n = 0
    with open('../text mining/final/sentiment_whisky_final.csv', newline='', encoding='utf8') as csvfile:
        # 讀取 CSV 檔案內容
        rows = csv.reader(csvfile)
        # 以迴圈輸出每一列
        for row in rows:
            if rows.line_num == 1:
                continue
            else:
                # 將資料寫入ES
                doc = {"whiskey_name": row[0], "text": row[1],"score": row[2], }
                # print(doc)
                res = es.index(index='whiskey_score', doc_type='_doc', body=doc)
                print(res['result'])
                n += 1
                print(n)




def put_timesearch():
    n=0
    with open('./whiskey_kibana_final_latlon_format_1.csv', newline='', encoding='utf8') as csvfile:
        # 讀取 CSV 檔案內容
        rows = csv.reader(csvfile)
        # 以迴圈輸出每一列
        for row in rows:
            if rows.line_num == 1:
                continue
            else:
                # 將資料寫入ES

                dt = datetime.datetime.strptime(row[5], "%Y-%m-%d %H:%M:%S")      #將時間轉換成es需要的格式
                mappings = {"whiskey_name": row[0], "user" : row[1], "text" : row[2], "language" : row[3],
                       "score" : row[4],  "abv" : row[6], "brand_country" : row[7], "official_content":row[8],
                       "type" : row[9], "year" : row[10], "datatime": dt , "location" : row[11]}
                print(mappings)
                res = es.index(index='whiskey_kibana_final_latlon_1', doc_type='_doc', body=mappings) #index=索引名稱 ， body需要注意 如果是座標圖 要跟宣告座標的key一樣
                print(res['result'])
                n+=1
                print(n)


if __name__ == "__main__":
    # put_timesearch()
    # put_comment_score()
    put_word_weight()
