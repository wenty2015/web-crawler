from storer import *

DOC = 'DOC'
DOCNO = 'DOCNO'
DEPTH = 'DEPTH'
HEAD = 'HEAD'
HTTPHEADER = 'HTTPHEADER'
OUTLINKS = 'OUTLINKS'
TEXT = 'TEXT'
HTML = 'HTML'

import os
dir_data = '../results/URL/'
file_list = os.listdir(dir_data)
print len(file_list), ' files to load'

from elasticsearch import Elasticsearch
es = Elasticsearch(['localhost'], # hostname
                   http_auth=('elastic', 'elastic')) # username, pwd
from datetime import datetime

now = datetime.now()

index = Store()
# insert(self, count, url, header, title, text, raw, out_links)
def getStart(text):
    return ''.join(['<', text, '>'])
def getEnd(text):
    return ''.join(['</', text, '>'])

cnt = 0
url_info = {}
for file_name in file_list:
    if file_name == 'readme':
        continue

    with open(dir_data+file_name) as f:
        for l in f:
            line = l.rstrip('\n')
            if line[:len('<DOC>')] == '<DOC>':
                if cnt > 0:
                    index.insert(doc_id, depth, head, out_links, text, html)
                read_text, read_html = False, False
                text, html = '', ''

                cnt += 1
                if cnt % 500 == 0:
                    print 'loaded ', cnt, ' documents'
            elif line[:len(getStart(DOCNO))] == getStart(DOCNO):
                doc_id = line.lstrip(getStart(DOCNO)).rstrip(getEnd(DOCNO)).strip(' ')
            elif line[:len(getStart(DEPTH))] == getStart(DEPTH):
                depth = line.lstrip(getStart(DEPTH)).rstrip(getEnd(DEPTH)).strip(' ')
            elif line[:len(getStart(HEAD))] == getStart(HEAD):
                head = line.lstrip(getStart(HEAD)).rstrip(getEnd(HEAD)).strip(' ')
            elif line[:len(getStart(OUTLINKS))] == getStart(OUTLINKS):
                out_links = line.lstrip(getStart(OUTLINKS)).rstrip(getEnd(OUTLINKS)).strip(' ')
            elif line[:len(getStart(TEXT))] == getStart(TEXT)):
                read_text = True
                if len(line.lstrip(getStart(TEXT))) > 0:
                    text += l.lstrip(getStart(TEXT))
            elif line[-len(getEnd(TEXT)):] == getEnd(TEXT):
                read_text = False
            elif line[:len(getStart(HTML))] == getStart(HTML)):
                read_html = True
                if len(line.lstrip(getStart(HTML))) > 0:
                    html += l.lstrip(getStart(HTML))
            elif line[-len(getEnd(HTML)):] == getEnd(HTML):
                read_html = False
            else:
                if read_text:
                    text += l
                elif read_html:
                    html += l
if cnt > 0:
    index.insert(doc_id, depth, head, out_links, text, html)

print 'running time is ', datetime.now() - now
print 'total number of documents loaded is', cnt
