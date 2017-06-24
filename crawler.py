from bs4 import BeautifulSoup
import urllib2
import robotparser
from collections import deque
import time
import os, re
from datetime import datetime
import threading
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

RESULT_DIR = '../results 3/'

class Crawler:
    MAX_URL_NUM = 1000
    URL_PER_FILE = 500
    FILE_URL = 'URL'
    def __init__(self, seed_url_list, title = ''):
        self.depth = 1
        self.url_num, self.domain_num = 0, 0
        self.file_no, self.file_cnt = 1, 0

        self.seed_url_list = seed_url_list
        self.domain_map = {} # {domain: domain_id}
        # {domain_id: {'domain':domain, 'robot':robot_parser}}
        self.domain_nodes = {}
        self.url_map = {} # {url: url_id}
        ''' url_nodes: {url_id: {'domain_id': domain_id, 'url': url,
                                'out_links': [out_link_url], in_link: (url_id)}}'''
        self.url_nodes = {}
        self.initializeSeedURL(title)

    def initializeSeedURL(self, title = ''):
        '''add seed urls to the queue, and initialize the url nodes'''
        self.url_queue = deque()
        self.title_terms = set()
        self.openNewHTMLFiles()
        if len(title) > 0:
            for term in re.findall(r"\w+",title):
                #self.title_terms.add(stem(term.lower()))
                self.title_terms.add(term.lower())
        print 'title', self.title_terms

        for raw_url in self.seed_url_list:
            url = canonicalizeURL(raw_url)
            domain = getDomain(url)
            html_content = self.loadHTML(url)
            _, url_id = self.set(url, domain, html_content)
            self.url_queue.append(url_id)
        return

    def loadHTMLContent(self, content, http_headers, url):
        html_content = {}
        html_content['html'] = content
        html_content['http_headers'] = http_headers
        # load html content
        soup = BeautifulSoup(content, 'lxml')
        html_content['out_links'] = self.getOutLinks(soup, url)
        title_set = soup.find_all('title')
        if title_set == None or len(title_set) == 0:
            html_content['title'] = []
        else:
            def stripTitle(title):
                return str(title).lstrip('<title>').rstrip('</title>')
            html_content['title'] = map(lambda t: stripTitle(t), title_set)
        html_content['text'] = soup.get_text()
        return html_content

    def loadHTML(self, url):
        try:
            url_open = urllib2.urlopen(url, timeout = 1)
            content = url_open.read()
            http_headers = url_open.info().headers
            return self.loadHTMLContent(content, http_headers, url)
        except urllib2.URLError as e:
            print 'network connection lost', url, e
            return None
        except ValueError as e:
            print 'can not open', url, e
            return None
        except urllib2.HTTPError as e:
            print 'http error for', url, e
            return None
        except:
            print 'unexpected error', url
            return None

    def getOutLinks(self, soup, parent_url):
        out_links = []
        for link in soup.find_all('a'):
            link_content = [link.get('href'), link.get('title')]
            href = link_content[0]
            if link_content[1] is None:
                link_terms = set()
            else:
                link_terms = re.findall(r"\w+", link_content[1].lower())
                # link_terms = set(map(lambda t: stem(t), link_terms))
                link_terms = set(link_terms)
            if href is not None and len(href) > 1 and isValieURLType(href) and \
                    href[0] != '#':
                # skip None href, invalide url type and fragment
                if self.title_terms is not None:
                    exist_in_url = False
                    for term in self.title_terms:
                        if term in link_content[0]:
                            exist_in_url = True
                            break
                    if len(self.title_terms & link_terms) > 0 or exist_in_url:
                        # get the full address
                        out_link_full = getOutLinkURL(link_content[0], parent_url)
                        if len(out_link_full) > 0:
                            out_links.append(out_link_full)
                else:
                    out_link_full = getOutLinkURL(link_content[0], parent_url)
                    out_links.append(out_link_full)
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
            print url_id, url
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
            self.dumpURL(html_content, url, url_id)
            self.url_nodes[url_id] = {'domain_id': domain_id, 'url': url,
                                        'out_links': html_content['out_links'],
                                        'in_links': set()}
            self.url_num += 1
        else:
            url_id = self.url_map[url]
        return url_id

    def openNewHTMLFiles(self):
        self.file_html = open(''.join([RESULT_DIR, 'URL/', self.FILE_URL, '_',
                                        str(self.file_no), '.txt']), 'wb')
        return

    def dumpURL(self, html_content, url, url_id):
        if self.file_cnt == self.URL_PER_FILE:
            self.file_html.close()
            self.file_no, self.file_cnt = self.file_no + 1, 0
            self.openNewHTMLFiles()
        self.file_cnt += 1
        self.file_html.write('<DOC>' + '\n')
        self.file_html.write(' '.join(['<DOCNO>', str(url_id), '</DOCNO>', '\n']))
        self.file_html.write(' '.join(['<URL>', url, '</URL>', '\n']))
        self.file_html.write(' '.join(['<DEPTH>', str(self.depth), '</DEPTH>', '\n']))
        if len(html_content['title']) > 0:
            title_text = ' '.join(html_content['title'])
        else:
            title_text = ''
        self.file_html.write(' '.join(['<HEAD>', title_text,
                                        '</HEAD>', '\n']))
        if len(html_content['http_headers']) > 0:
            http_headers_text = ' '.join(html_content['http_headers'])
        else:
            http_headers_text = ''
        self.file_html.write(' '.join(['<HTTPHEADER>', '\n',
                                        http_headers_text,
                                        '</HTTPHEADER>', '\n']))
        # print html_content['out_links']
        out_links_text = ' '.join(html_content['out_links'])
        self.file_html.write(' '.join(['<OUTLINKS>', out_links_text,
                                        '</OUTLINKS>', '\n']))
        self.file_html.write(' '.join(['<TEXT>', '\n',
                                        html_content['text'],
                                        '\n', '</TEXT>', '\n']))
        self.file_html.write(' '.join(['<HTML>', '\n',
                                        html_content['html'],
                                        '\n', '</HTML>', '\n']))
        self.file_html.write('</DOC>' + '\n')
        return

    def crawl(self):
        now = datetime.now()
        while self.url_num < self.MAX_URL_NUM:
            print 'depth', self.depth, 'urls in the queue', len(self.url_queue)
            # print 'links in the queue', len(self.url_queue)
            # get the url list for the next wave of BFS
            url_next_level = self.processURLQueue()
            if len(url_next_level) > 0:
                '''aggregate out links
                out_links: {tmp_url_id: {'url': url, 'in_links': [in_link_url_id],
                                            'domain_id': domain_id, 'duration': i}}'''
                print 'process outlinks'
                out_links = processOutLinks(url_next_level)
                # print out_links
                ''' sort the out links for each domain
                crawl_list: {domain_id: [tmp_url_id]}'''
                print 'generate crawl list'
                crawl_list = processCrawlList(out_links)
                '''with open(RESULT_DIR + 'url_next_level', 'wb') as f:
                    cPickle.dump(url_next_level, f)
                with open(RESULT_DIR + 'out_links', 'wb') as f:
                    cPickle.dump(out_links, f)
                with open(RESULT_DIR + 'crawl_list', 'wb') as f:
                    cPickle.dump(crawl_list, f)'''

                # crawl all the urls in the next wave, and add them as url nodes
                print 'start crawling next wave'
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
        self.file_html.close()
        print 'running time', datetime.now() - now
        return

    def processURLQueue(self):
        '''get the list of all out links, which have not been crawled and
        is allowed to visit.'''
        url_next_level = []
        while self.url_queue:
            url_id = self.url_queue.popleft()
            url = self.url_nodes[url_id]['url']
            # print 'process url in the queue', url
            out_links = self.url_nodes[url_id]['out_links']
            for out_link in out_links:
                # clean and check out link
                # print 'process out link', out_link
                out_link_domain = getDomain(out_link)
                out_link_domain_id = self.addDomain(out_link_domain)
                if out_link == '': # not valid url
                    continue
                elif self.canCrawl(out_link, out_link_domain) == False:
                    # follow politeness
                    continue
                elif out_link in self.url_map:
                    # skip the existing url, but add the edge
                    self.addInLink(out_link, url_id)
                    continue
                else:
                    ''' [out_link, in_link_url_id, out_link_domain_id]'''
                    out_link_info = [out_link, url_id, out_link_domain_id]
                    url_next_level.append(out_link_info)
        return url_next_level

    def addInLink(self, url, in_link_url_id):
        url_id = self.url_map[url]
        self.url_nodes[url_id]['in_links'].add(in_link_url_id)
        return

    def crawlNextLevel(self, crawl_list, out_links):
        ''' crawl the urls in crawl_list and add them to the url_queue
        input:
        crawl_list: {domain_id: [tmp_url_id]},
        out_links: {tmp_url_id: {'url': url, 'in_links': [url_id],
                                'domain_id': domain_id, 'duration': i}}'''
        list_length = max(map(lambda l: len(l[1]), crawl_list.items()))
        for length in xrange(1, list_length + 1):
            crawl_list_present = filter(lambda l: len(l[1]) >= length,
                                        crawl_list.items())
            crawl_list_present = map(lambda l: l[1][length - 1], crawl_list_present)
            for tmp_url_id in crawl_list_present:
                '''t = threading.Thread(target = self.crawlURLNextLevel,
                                    args = (tmp_url_id, out_links))
                t.daemon = True
                t.start()'''
                self.crawlURLNextLevel(tmp_url_id, out_links)
                if self.url_num > self.MAX_URL_NUM:
                    # stop when meets the requirement
                    return
            # wait .5s to avoid visit the same domain too frequently
            time.sleep(.5)
        return

    def crawlURLNextLevel(self, tmp_url_id, out_links):
        url = out_links[tmp_url_id]['url']
        html_content = self.loadHTML(url)
        if html_content is None: # skip the page can not be opened
            return
        elif len(html_content['text']) == 0: # skip the page with no content
            return
        else:
            domain_id = out_links[tmp_url_id]['domain_id']
            domain = self.domain_nodes[domain_id]['domain']
            _, url_id = self.set(url, domain, html_content)
            for in_link in out_links[tmp_url_id]['in_links']:
                self.url_nodes[url_id]['in_links'].add(in_link)
            self.url_queue.append(url_id) # add the new url to the queue
        return

    def dumpCrawler(self):
        with open(RESULT_DIR + 'STATS.txt', 'wb') as f:
            f.write(' '.join(['depth', str(self.depth), '\n']))
            f.write(' '.join(['domain', str(self.domain_num), '\n']))
            f.write(' '.join(['url', str(self.url_num), '\n']))
            f.write(' '.join(self.seed_url_list))
        with open(RESULT_DIR + 'URL_MAP.txt', 'wb') as f:
            dumpDict(f, self.url_map)
        with open(RESULT_DIR + 'DOMAIN_MAP.txt', 'wb') as f:
            dumpDict(f, self.domain_map)
        self.dumpURLInfo(RESULT_DIR)
        return

    def dumpURLInfo(self, result_dir):
        f_url_info = open(result_dir + 'URL_INFO.txt', 'wb')
        for url_id, url_info in sorted(self.url_nodes.iteritems(), key = lambda x: x[0]):
            # f_url_info
            domain_id = url_info['domain_id']
            basic_info = [url_id, domain_id, url_info['url']]
            in_link = list(url_info['in_links'])
            f_url_info.write(listToText(basic_info, ' ') + ' ' +
                                listToText(in_link, ',') + '\n')
        f_url_info.close()
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
            # print 'error when parse robot', url
            return True

    def processRemainEdges(self):
        while self.url_queue:
            url_id = self.url_queue.popleft()
            url = self.url_nodes[url_id]['url']
            out_links = self.url_nodes[url_id]['out_links']
            for out_link in out_links:
                if out_link in self.url_map:
                    self.addInLink(out_link, url_id)
        return

def listToText(l, sep = ' '):
    return sep.join(map(lambda x: str(x), l))

def getOutLinkURL(out_link, parent_url):
    if out_link[:2] == '//': # new domain
        outlink = out_link.lstrip('//')
    elif out_link[:1] == '#': # fragment
        out_link = parent_url
    elif out_link[:1] == '/':
        domain = getDomain(parent_url)
        out_link = domain + out_link
    elif out_link[:4].lower() != 'http': # relative url
        out_link = getRelativeURL(parent_url, out_link)
    return canonicalizeURL(out_link)

def dumpDict(f, d):
    for key, value in sorted(d.items(), key = lambda x: x[0]):
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

def getDomain(url): # canonicalized url
    net_split = url.lower().split('://')
    if len(net_split) == 1: # no scheme
        net = net_split[0].split('/')
        return net[0]
    elif len(net_split) == 2:
        scheme, net = net_split
        net = net.split('/')
        return '://'.join([scheme, net[0]])
    else:
        return ''

def canonicalizeURL(url):
    net_split = url.lower().split('://')
    if len(net_split) == 1:
        scheme, net = '', net_split[0]
    elif len(net_split) == 2:
        scheme, net = net_split
    else:
        return ''
    # remove duplicate slashes and the fragment
    net = net.replace('//','/').split('#')[0].split('/')
    # remove port
    net[0] = net[0].split(':')[0]
    if scheme == '':
        url = '/'.join(net)
    else:
        url = '://'.join([scheme, '/'.join(net)])
    # domain = getDomain(url)
    return url

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
        elif relative_repo[:1] == '#':
            relative_url = url
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
    ''' url_next_level: [[out_link_url, in_link_url_id, out_link_domain_id]]'''
    tmp_url_no, no = {}, 0
    for url_info in url_next_level:
        if url_info[0] not in tmp_url_no:
            tmp_url_no[url_info[0]] = no
            no += 1
    url_indices = map(lambda l: tmp_url_no[l[0]], url_next_level)
    out_links = {}
    for i, out_link in enumerate(zip(url_next_level, url_indices)):
        url, in_link_url_id, domain_id = out_link[0]
        tmp_url_id = out_link[1]
        if tmp_url_id not in out_links:
            out_links[tmp_url_id] = {'url': url, 'in_links': [in_link_url_id],
                                    'domain_id': domain_id, 'duration': i}
        else:
            out_links[tmp_url_id]['in_links'].append(in_link_url_id)
    return out_links

def processCrawlList(out_links):
    '''out_links: {tmp_url_id: {'url': url, 'in_links': [in_link_url_id],
                                'domain_id': domain_id, 'duration': i}}'''
    # sort by number of in link descending, duration ascending
    sorted_out_links = sorted(out_links.items(),
                                key = lambda x: (-len(x[1]['in_links']),
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
    seed_url_list = ['http://en.wikipedia.org/wiki/American_Revolution'
    ''',
    'http://www.revolutionary-war.net/causes-of-the-american-revolution.html',
    'http://www.historycentral.com/Revolt/causes.html',
    'https://lenoxhistory.org/lenoxhistorybigpicture/non-importation-agreement/',
    'http://www.history.com/topics/american-revolution/american-revolution-history''''
    ]
    title = 'independence war american revolution cause causes reason reasons purpose purposes'
    crawler = Crawler(seed_url_list, title)
    crawler.crawl()
