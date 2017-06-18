from bs4 import BeautifulSoup
import urllib2
import robotparser
from collections import deque

class Crawler:
    def __init__(self, seed_url_list):
        self.seed_url_list = seed_url_list
        self.link_nodes = {} # {domain: domain_id}
        self.link_edges = [] # [[inlink_id, outlink_id]]
        self.content = {} # {domain_id: html}
        self.queue = deque(self.seed_url_list)
        self.depth = 1

def canonicalizeURL(url):
    scheme, net = url.lower().split('://')
    # remove duplicate slashes and the fragment
    net = net.replace('//','/').split('#')[0].split('/')
    # remove port
    net[0] = net[0].split(':')[0]
    domain = '://'.join([scheme, net[0]])
    url = '://'.join([scheme, '/'.join(net)])
    return domain, url

def getRelativeURL(url, relative_repo = ''):
    if len(relative_repo) > 0:
        if relative_repo[:2] == '..':
            net = url.split('/')
            relative = relative_repo.split('/')
            parent_level = 0
            while relative[parent_level] == '..':
                if parent_level == 0:
                    net.pop()
                parent_level += 1
                net.pop()
            relative_url = '/'.join(net + relative[parent_level:])
        else:
            relative_url = url + relative_repo
    return relative_url

def isValieURLType(url):
    type_not_covered = ['pdf', 'jpg', 'jpeg', 'gif', 'png', 'svg', 'zip']
    if url.split('.')[-1] in type_not_covered:
        return False
    else:
        return True

seed_url_list = ['http://en.wikipedia.org/wiki/American_Revolution']

# for seed_url in seed_url_list:
