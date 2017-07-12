from elasticsearch import Elasticsearch
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
from datetime import datetime

RESULT_DIR = 'results/'
f = open(RESULT_DIR + 'merged_index_in_links.txt', 'wb')

client = Elasticsearch()
index = 'crawler_beauty1'
doc_type = 'document'
cnt = 0
now = datetime.now()

def getScrollInfo(es, size = 1000):
    results =  es.search(
        index = index, doc_type = doc_type, size = size,
        scroll = '1m',
        body = { "query":{
                    "match_all":{ } },
                 "script_fields":{
                    "url":{
                          "script":{
                          "lang": "groovy",
                          "inline": "_source['url']" }
                    },
                    "in_links":{
                          "script":{
                          "lang": "groovy",
                          "inline": "_source['in_links']" }
                    }
                }
            })
    return results

def getInLinks(in_links_data, f, cnt, urls):
    for doc in in_links_data:
        url = doc['_id'].replace(' ', '_').replace('\n', '_')
        in_links = []
        if 'in_links' in doc['fields'].keys():
            in_links_raw = doc['fields']['in_links']
            if len(in_links_raw) > 0:
                for il in in_links_raw:
                    if il is not None:
                        il = il.replace(' ', '_').replace('\n', '_')
                        if il in urls:
                            in_links.append(il)
        else:
            print url, 'has no in links'
        f.write(' '.join([url] + in_links + ['\n']))
        cnt += 1
        if cnt % 1000 == 0:
            print cnt, 'urls'
    return cnt

def loadURLs():
    f = open(RESULT_DIR + 'merged_index_urls.txt', 'r')
    urls = set()
    for url in f.readlines():
        urls.add(url.rstrip('\n'))
    return urls

urls = loadURLs()
scroll = getScrollInfo(client)
scroll_id = scroll['_scroll_id']
while True:
    if len(scroll['hits']['hits']) == 0:
        break
    else:
        # print scroll['hits']['hits']
        cnt = getInLinks(scroll['hits']['hits'], f, cnt, urls)
    scroll = client.scroll(scroll_id = scroll_id, scroll= "1m")
    scroll_id = scroll['_scroll_id']

print 'running time', datetime.now() - now
f.close()
