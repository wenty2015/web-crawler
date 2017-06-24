from storer import *

DOC = 'DOC'
DOCNO = 'DOCNO'
URL = 'URL'
DEPTH = 'DEPTH'
HEAD = 'HEAD'
HTTP_HEADER = 'HTTPHEADER'
OUTLINKS = 'OUTLINKS'
TEXT = 'TEXT'
HTML = 'HTML'

import os
DIR_DATA = '../results/'

DIR_FILE = DIR_DATA + 'URL/'
file_list = os.listdir(DIR_FILE)[:1]
print len(file_list), ' files to load'

'''from elasticsearch import Elasticsearch
es = Elasticsearch(['localhost'], # hostname
                   http_auth=('elastic', 'elastic')) # username, pwd
'''
from datetime import datetime

now = datetime.now()

index = Store()
# insert(self, count, url, header, title, text, raw, out_links)
def getStart(text):
    return ''.join(['<', text, '>'])
def getEnd(text):
    return ''.join(['</', text, '>'])

def loadURLInfo():
    with open(DIR_DATA + 'URL_INFO.txt', 'rb') as f:
        f_content = f.readlines()
    url_info = {}
    for content in f_content:
        url_id, domain_id, url, in_links = content.rstrip('\n').split(' ')
        in_links_list = in_links.split(',')
        url_info[url_id] = in_links_list
    return url_info

def loadURLMap():
    with open(DIR_DATA + 'URL_MAP.txt', 'rb') as f:
        f_content = f.readlines()
    url_map = {}
    for content in f_content:
        url_id, url = content.rstrip('\n').split(' ')
        url_map[url_id] = url
    return url_map

cnt = 0
url_info = loadURLInfo()
url_map = loadURLMap()

for file_name in file_list:
    if file_name == 'readme':
        continue

    with open(DIR_FILE+file_name) as f:
        for l in f:
            line = l.rstrip('\n').rstrip(' ')
            if line[:len('<DOC>')] == '<DOC>':
                if cnt > 0:
                    in_links = map(lambda i: url_map[i] if i != '' else '',
                                    url_info[url_id])
                    index.insert(url, url_id, http_header, head, text, html,
                                    in_links, out_links, depth)
                read_text, read_html, read_http_header = False, False, False
                text, html, http_header = '', '', ''

                cnt += 1
                if cnt % 500 == 0:
                    print 'loaded ', cnt, ' documents'
            elif line[:len(getStart(DOCNO))] == getStart(DOCNO):
                url_id = line.lstrip(getStart(DOCNO)).rstrip(getEnd(DOCNO)).strip(' ')
            elif line[:len(getStart(URL))] == getStart(URL):
                url = line.lstrip(getStart(URL)).rstrip(getEnd(URL)).strip(' ')
            elif line[:len(getStart(DEPTH))] == getStart(DEPTH):
                depth = line.lstrip(getStart(DEPTH)).rstrip(getEnd(DEPTH)).strip(' ')
            elif line[:len(getStart(HEAD))] == getStart(HEAD):
                head = line.lstrip(getStart(HEAD)).rstrip(getEnd(HEAD)).strip(' ')
            elif line[:len(getStart(OUTLINKS))] == getStart(OUTLINKS):
                out_links = line.lstrip(getStart(OUTLINKS)).rstrip(getEnd(OUTLINKS)).strip(' ')

            elif line[:len(getStart(TEXT))] == getStart(TEXT):
                read_text = True
                if len(line.lstrip(getStart(TEXT))) > 0:
                    text += l.lstrip(getStart(TEXT))
            elif line[-len(getEnd(TEXT)):] == getEnd(TEXT):
                read_text = False

            elif line[:len(getStart(HTML))] == getStart(HTML):
                read_html = True
                if len(line.lstrip(getStart(HTML))) > 0:
                    html += l.lstrip(getStart(HTML))
            elif line[-len(getEnd(HTML)):] == getEnd(HTML):
                read_html = False

            elif line[:len(getStart(HTTP_HEADER))] == getStart(HTTP_HEADER):
                read_http_header = True
                if len(line.lstrip(getStart(HTTP_HEADER))) > 0:
                    http_header += l.lstrip(getStart(HTTP_HEADER))
            elif line[-len(getEnd(HTTP_HEADER)):] == getEnd(HTTP_HEADER):
                read_http_header = False

            else:
                if read_text:
                    text += l
                elif read_html:
                    html += l
                elif read_http_header:
                    http_header += l
if cnt > 0:
    in_links = map(lambda i: url_map[i] if i != '' else '',
                    url_info[url_id])
    index.insert(url, url_id, http_header, head, text, html,
                    in_links, out_links, depth)

print 'running time is ', datetime.now() - now
print 'total number of documents loaded is', cnt
