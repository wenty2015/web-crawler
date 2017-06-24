from elasticsearch import Elasticsearch

class Store:

    def __init__(self):
        self.client = Elasticsearch()
        self.index = 'crawler_w'
        self.doc_type = 'document'

    def insert(self, url, url_id, http_header, title, text, html,
                in_links, out_links, depth):
        try:
            body = {
                'docno': url_id,
                'HTTPheader': http_header,
                'title': title,
                'text': text,
                'html_Source': html,
                'in_links': in_links,
                'out_links': out_links,
                'author': 'wqin',
                'depth': depth,
                'url': url
            }

            self.client.index(index = self.index, doc_type = self.doc_type,
                              id = url, body = body)

        except Exception as e:
            print 'ES index exception: {}'.format(e)
