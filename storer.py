from elasticsearch import Elasticsearch

class Store:

    def __init__(self, index_name):
        self.client = Elasticsearch()
        self.index = index_name
        self.doc_type = 'document'

    def mergeInLinks(self, url, url_id, http_header, title, text, html,
                        in_links, out_links, depth):
        if self.search(url) == 0:
            self.insert(url, url_id, http_header, title, text, html,
                        in_links, out_links, depth)
        else:
            existing_in_links = self.get(url, 'in_links')
            new_in_links = set(in_links) - set(existing_in_links)
            if len(new_in_links) > 0:
                self.update(list(new_in_links), url)
        return

    def search(self, url):
        result = self.client.search(index = self.index, doc_type = self.doc_type,
                                    body = { "query": {
                                             "match": { "_id": url }
                                             }})['hits']['total']
        return result

    def get(self, url, field):
        data = self.client.search(index = self.index, doc_type = self.doc_type,
                body = { "query": {
                         "match": {"_id": url}
                          }})['hits']['hits'][0]['_source'][field]
        return data

    def update(self, new_in_links, url):
        try:
            self.client.update(index = self.index, doc_type = self.doc_type, id = url,
              body = {"query":{
                        "script" : {
                            "inline": "ctx._source.in_links.addAll(params.new_in_links)",
                            "lang": "painless",
                            "params" : {"new_in_links" : new_in_links}}}})
        except Exception as e:
            print url, e
        return

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
            print url, e
