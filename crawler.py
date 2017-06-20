from bs4 import BeautifulSoup
import urllib2
import robotparser
from collections import deque
import time
import numpy as np
import os
import pdb
import re
from datetime import datetime
import cPickle
from stemming.porter2 import stem

class Crawler:
    MAX_URL_NUM = 1000
    def __init__(self, seed_url_list, title = ''):
        self.depth = 1
        self.url_num, self.domain_num = 0, 0
        self.seed_url_list = seed_url_list
        self.domain_map = {} # {domain: domain_id}
        # {domain_id: {'domain':domain, 'robot':robot_parser}}
        self.domain_nodes = {}
        self.url_map = {} # {url: url_id}
        ''' url_nodes: {url_id: {'domain_id': domain_id, 'url': url,
                                'html': html_content, in_link: (url_id)}}'''
        self.url_nodes = {}
        self.initializeSeedURL(title)

    def initializeSeedURL(self, title = ''):
        '''add seed urls to the queue, and initialize the url nodes'''
        self.url_queue = deque()
        self.title_terms = set()
        if len(title) > 0:
            for term in re.findall(r"\w+",title):
                self.title_terms.add(stem(term.lower()))
        print 'title', self.title_terms

        for raw_url in self.seed_url_list:
            domain, url = canonicalizeURL(raw_url)
            html_content = self.loadHTML(url)
            _, url_id = self.set(url, domain, html_content)
            self.url_queue.append(url_id)
        return

    def loadHTML(self, url):
        try:
            content = urllib2.urlopen(url).read()
        except urllib2.URLError as e:
            try:
                content = urllib2.urlopen(url).read()
            except:
                print 'network connection lost', e
                return None
        except ValueError as e:
            print 'can not open', url, e
            return None
        except urllib2.HTTPError as e:
            print 'http error for', url, e
            return None
        else:
            print 'unexpected error'
            return None
        html_content = {}
        html_content['html'] = content
        # load html content
        soup = BeautifulSoup(content, 'xml')
        html_content['out_links'] = self.getOutLinks(soup)
        html_content['title'] = soup.find_all('title')
        html_content['text'] = soup.get_text()
        return html_content

    def getOutLinks(self, soup):
        out_links = []
        for link in soup.find_all('a'):
            link_content = [link.get('href'), link.get('title')]
            href = link_content[0]
            if link_content[1] is None:
                link_terms = set()
            else:
                link_terms = re.findall(r"\w+", unicode(link_content[1]).lower())
                link_terms = set(map(lambda t: stem(t), link_terms))
            if href is not None and len(href) > 1 and isValieURLType(href) and href[0] != '#':
                # skip None href, invalide url type and fragment
                if self.title_terms is not None:
                    if len(self.title_terms & link_terms) > 0:
                        out_links.append(link_content)
                else:
                    out_links.append(link_content)
        return out_links

    def set(self, url, domain, html_content):
        if url in self.url_map:
            return -1, -1
        else:
            if domain not in self.domain_map:
                domain_id = self.addDomain(domain)
            else:
                domain_id = self.domain_map[domain]
            url_id = self.addURL(url, domain_id, html_content)
            return domain_id, url_id

    def addDomain(self, domain):
        if domain not in self.domain_map:
            domain_id = self.domain_num
            self.domain_map[domain] = domain_id
            self.domain_nodes[domain_id] = {'domain': domain,
                                            'robot': parseRobot(domain)}
            self.domain_num += 1
        else:
            domain_id = self.domain_map[domain]
        return domain_id

    def addURL(self, url, domain_id, html_content):
        if url not in self.url_map:
            url_id = self.url_num
            self.url_map[url] = url_id
            self.url_nodes[url_id] = {'domain_id': domain_id, 'url': url,
                                        'html': html_content, 'in_link': set()}
            self.url_num += 1
        else:
            url_id = self.url_map[url]
        return url_id

    def crawl(self):
        now = datetime.now()
        while self.url_num < self.MAX_URL_NUM:
            print 'depth', self.depth
            print 'links in the queue', len(self.url_queue)
            # get the url list for the next wave of BFS
            url_next_level = self.processURLQueue()
            if len(url_next_level) > 0:
                '''aggregate out links
                out_links: {tmp_url_id: {'url': url, 'in_link': [in_link_url_id],
                                            'domain_id': domain_id, 'duration': i}}'''
                out_links = processOutLinks(url_next_level)
                # print out_links
                ''' sort the out links for each domain
                crawl_list: {domain_id: [tmp_url_id]}'''
                crawl_list = processCrawlList(out_links)
                with open('results/url_next_level', 'wb') as f:
                    cPickle.dump(url_next_level, f)
                with open('results/out_links', 'wb') as f:
                    cPickle.dump(out_links, f)
                with open('results/crawl_list', 'wb') as f:
                    cPickle.dump(crawl_list, f)

                # crawl all the urls in the next wave, and add them as url nodes
                self.crawlNextLevel(crawl_list, out_links)
                self.depth += 1
            else:
                print 'no new out links'
        # add out link edges of the url remaining in the url_queue
        print 'add remaining link edges'
        self.processRemainEdges()
        # save results
        print 'write results'
        self.dumpCrawler()
        print 'running time', datetime.now() - now
        return

    def processURLQueue(self):
        '''get the list of all out links, which have not been crawled and
        is allowed to visit.'''
        url_next_level = []
        while self.url_queue:
            url_id = self.url_queue.popleft()
            url = self.url_nodes[url_id]['url']
            print 'process url in the queue', url
            out_links = self.url_nodes[url_id]['html']['out_links']
            for out_link, out_link_title in out_links:
                # clean and check out link
                # print 'process out link', out_link
                out_link_domain, out_link_url = getOutLinkURL(out_link, url)
                out_link_domain_id = self.addDomain(out_link_domain)
                if out_link_url == '': # not valid url
                    continue
                elif self.canCrawl(out_link_url, out_link_domain) == False:
                    # follow politeness
                    continue
                elif out_link_url in self.url_map:
                    # skip the existing url, but add the edge
                    self.addInLink(out_link_url, url_id)
                    continue
                else:
                    ''' [out_link_url, in_link_url_id, out_link_title,
                        out_link_domain_id]'''
                    out_link_info = [out_link_url, url_id, out_link_title,
                                    out_link_domain_id]
                    url_next_level.append(out_link_info)
        return url_next_level

    def addInLink(self, url, in_link_url_id):
        url_id = self.url_map[url]
        self.url_nodes[url_id]['in_link'].add(in_link_url_id)
        return

    def crawlNextLevel(self, crawl_list, out_links):
        ''' crawl the urls in crawl_list and add them to the url_queue
        input:
        crawl_list: {domain_id: [tmp_url_id]},
        out_links: {tmp_url_id: {'url': url, 'in_link': [url_id],
                                'domain_id': domain_id, 'duration': i}}'''
        list_length = max(map(lambda l: len(l[1]), crawl_list.items()))
        for length in xrange(1, list_length + 1):
            crawl_list_present = filter(lambda l: len(l[1]) >= length,
                                        crawl_list.items())
            crawl_list_present = map(lambda l: l[1][length - 1], crawl_list_present)
            for tmp_url_id in crawl_list_present:
                url = out_links[tmp_url_id]['url']
                html_content = self.loadHTML(url)
                if html_content is None: # skip the page can not be opened
                    continue
                elif len(html_content['text']) == 0: # skip the page with no content
                    continue
                else:
                    print 'url for the next level, number of out links',\
                            len(html_content['out_links'])

                    domain_id = out_links[tmp_url_id]['domain_id']
                    domain = self.domain_nodes[domain_id]['domain']
                    _, url_id = self.set(url, domain, html_content)
                    for in_link in out_links[tmp_url_id]['in_link']:
                        self.url_nodes[url_id]['in_link'].add(in_link)
                    self.url_queue.append(url_id) # add the new url to the queue
                    if self.url_num > self.MAX_URL_NUM:
                        # stop when meets the requirement
                        return
            # wait .5s to avoid visit the same domain too frequently
            time.sleep(.5)
        return

    def dumpCrawler(self):
        result_dir = 'results/'
        with open(result_dir + 'STATS.txt', 'wb') as f:
            f.write(' '.join(['depth', str(self.depth), '\n']))
            f.write(' '.join(['domain', str(self.domain_num), '\n']))
            f.write(' '.join(['url', str(self.url_num), '\n']))
            f.write(' '.join(self.seed_url_list))
        with open(result_dir + 'URL_MAP.txt', 'wb') as f:
            dumpDict(f, self.url_map)
        with open(result_dir + 'DOMAIN_MAP.txt', 'wb') as f:
            dumpDict(f, self.domain_map)
        self.dumpURLInfo(result_dir)
        return

    def dumpURLInfo(self, result_dir):
        f_url_info = open(result_dir + 'URL_INFO.txt', 'wb')
        f_html_content = open(result_dir + 'HTML.txt', 'wb')
        f_html_content_offset = open(result_dir + 'HTML_OFFSET.txt', 'wb')
        for url_id, url_info in sorted(self.url_nodes.iteritems(), key = lambda x: x[0]):
            # f_url_info
            domain_id = url_info['domain_id']
            basic_info = [url_id, domain_id,
                            url_info['url']]
            in_link = list(url_info['in_link'])
            f_url_info.write(listToText(basic_info, ' ') + ' ' +
                                listToText(in_link, ',') + '\n')
            # f_html_content
            offset = f_html_content.tell()
            f_html_content.write(self.url_nodes[url_id]['html']['html'] + '\n')
            length = f_html_content.tell() - offset
            offset_info = [url_id, offset, length]
            f_html_content_offset.write(listToText(offset_info, ' ') + '\n')
        f_url_info.close()
        f_html_content.close()
        f_html_content_offset.close()
        return

    def canCrawl(self, url, domain):
        if domain in self.domain_map:
            domain_id = self.domain_map[domain]
            robot_parser = self.domain_nodes[domain_id]['robot']
        else:
            robot_parser = parseRobot(domain)
        if robot_parser is None:
            return True
        try:
            return robot_parser.can_fetch('*', url)
        except:
            print 'error when parse robot', url

    def processRemainEdges(self):
        while self.url_queue:
            url_id = self.url_queue.popleft()
            url = self.url_nodes[url_id]['url']
            out_links = self.url_nodes[url_id]['html']['out_links']
            for out_link, out_link_title in out_links:
                # clean and check out link
                out_link_domain, out_link_url = getOutLinkURL(out_link, url)
                if out_link_url in self.url_map:
                    self.addInLink(out_link_url, url_id)
        return

def listToText(l, sep = ' '):
    return sep.join(map(lambda x: str(x), l))

def getOutLinkURL(out_link, parent_url):
    if out_link[:2] == '//': # new domain
        outlink = out_link.lstrip('//')
    elif out_link[:1] == '#': # fragment
        out_link = parent_url
    elif out_link[:1] == '/':
        domain, _ = canonicalizeURL(parent_url)
        out_link = domain + out_link
    elif out_link[:4].lower() != 'http': # relative url
        out_link = getRelativeURL(parent_url, out_link)
    return canonicalizeURL(out_link)

def dumpDict(f, d):
    for key, value in d.iteritems():
        line_list = [value, key]
        line = ' '.join(map(lambda t: str(t), line_list)) + '\n'
        f.write(line)
    return

def parseRobot(domain):
    robot_url = '/'.join([domain, 'robots.txt'])
    robot_parser = robotparser.RobotFileParser()
    robot_parser.set_url(robot_url)
    try:
        robot_parser.read()
        return robot_parser
    except:
        return None

def canonicalizeURL(url):
    net_split = url.lower().split('://')
    if len(net_split) == 1:
        scheme, net = '', net_split[0]
    elif len(net_split) == 2:
        scheme, net = net_split
    else:
        return '', ''
    # remove duplicate slashes and the fragment
    net = net.replace('//','/').split('#')[0].split('/')
    # remove port
    net[0] = net[0].split(':')[0]
    if scheme == '':
        domain = net[0]
        url = '/'.join(net)
    else:
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
    type_not_covered = ['pdf', 'jpg', 'jpeg', 'gif', 'png', 'svg', 'zip', 'doc']
    if url.split('.')[-1] in type_not_covered:
        return False
    else:
        return True

def processOutLinks(url_next_level):
    ''' url_next_level: [[out_link_url, in_link_url_id, out_link_title,
        out_link_domain_id]]'''
    tmp_url_no, no = {}, 0
    for url_info in url_next_level:
        if url_info[0] not in tmp_url_no:
            tmp_url_no[url_info[0]] = no
            no += 1
    url_indices = map(lambda l: tmp_url_no[l[0]], url_next_level)
    out_links = {}
    for i, out_link in enumerate(zip(url_next_level, url_indices)):
        url, in_link_url_id, title, domain_id = out_link[0]
        tmp_url_id = out_link[1]
        if tmp_url_id not in out_links:
            out_links[tmp_url_id] = {'url': url, 'in_link': [in_link_url_id],
                                    'domain_id': domain_id, 'duration': i}
        else:
            out_links[tmp_url_id]['in_link'].append(in_link_url_id)
    return out_links

def processCrawlList(out_links):
    '''out_links: {tmp_url_id: {'url': url, 'in_link': [in_link_url_id],
                                'domain_id': domain_id, 'duration': i}}'''
    # sort by number of in link descending, duration ascending
    sorted_out_links = sorted(out_links.items(),
                                key = lambda x: (-len(x[1]['in_link']),
                                                x[1]['duration']))
    crawl_list = {}
    for out_link in sorted_out_links:
        tmp_url_id = out_link[0]
        domain_id = out_link[1]['domain_id']
        if domain_id not in crawl_list:
            crawl_list[domain_id] = [tmp_url_id]
        else:
            crawl_list[domain_id].append(tmp_url_id)
    return crawl_list

if __name__ == '__main__':
    seed_url_list = ['http://en.wikipedia.org/wiki/American_Revolution',
    'http://www.revolutionary-war.net/causes-of-the-american-revolution.html',
    'http://teachinghistory.org/history-content/beyond-the-textbook/25627',
    'http://www.historycentral.com/Revolt/causes.html']
    title = unicode('independence war cause, american revolution reason')
    crawler = Crawler(seed_url_list, title)
    crawler.crawl()
