import robotparser
from collections import deque
import time
import os, re
from datetime import datetime
import requests
from lxml import html, cssselect
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import robotexclusionrulesparser, urllib2

#RESULT_DIR = '../results_full_20th/'
RESULT_DIR = '../results_test/'
class Crawler:
    MAX_URL_NUM = 21000
    URL_PER_FILE = 500
    FILE_URL = 'URL'
    def __init__(self, seed_url_list, title = ''):
        self.depth = 1
        self.url_num, self.domain_num = 0, 0
        self.file_no, self.file_cnt = 1, 0

        self.css_selector = cssselect.CSSSelector("a")

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
            self.title_terms = set(title.lower().split(' '))
        print 'title', self.title_terms

        for raw_url in self.seed_url_list:
            url = canonicalizeURL(raw_url)
            domain = getDomain(url)
            domain_id = self.addDomain(domain)
            if self.canCrawl(url, domain) == True:
                if self.domain_nodes[domain_id]['delay'] is None:
                    crawler_delay = 0.5
                else:
                    crawler_delay = self.domain_nodes[domain_id]['delay']
                html_content = self.loadHTML(url)
                if html_content is not None:
                    _, url_id = self.set(url, domain, html_content)
                self.url_queue.append(url_id)
                time.sleep(crawler_delay)
        return

    def loadHTMLContent(self, content, http_headers, url):
        html_content = {}
        html_content['html'] = content
        html_content['http_headers'] = dictToText(http_headers)
        # load html content
        html_parser = html.fromstring(content)
        html_content['out_links'] = self.getOutLinks(
                        self.css_selector(html_parser), url, http_headers)
        title = html_parser.findtext('.//title')
        html_content['title'] = '' if title is None else title
        html_content['text'] = html_parser.text_content()
        return html_content

    def loadHTML(self, url):
        try:
            url_open = requests.get(url, timeout = 0.5)
            content = url_open.text
            return self.loadHTMLContent(content, url_open.headers, url)
        except Exception as e:
            print e, url
            return None

    def getOutLinks(self, selector, parent_url, http_headers):
        out_links = []
        for link in selector:
            link_content = [link.get('href'), link.get('title'), link.get('rel')]
            href = link_content[0]
            nofollow_flag = link_content[2]
            link_terms = '' if link_content[1] is None else link_content[1].lower()
            can_crawl = href is not None \
                            and len(href) > 1 \
                            and isValieURLType(href) \
                            and href[0] != '#' \
                            and nofollow_flag != 'nofollow'
            valid_http_header = isValidHTTPHeader(http_headers)
            if can_crawl and valid_http_header:
                out_link_full = getOutLinkURL(link_content[0], parent_url)
                is_eligible_url = isEligibleURL(out_link_full)
                # skip None href, invalide url type and fragment
                if self.title_terms is not None:
                    exist_in_url, exist_in_title = 0, 0
                    for term in self.title_terms:
                        if term in out_link_full.lower():
                            exist_in_url += 1
                        if term in link_terms.lower():
                            exist_in_title += 1
                    if exist_in_title > 1 or exist_in_url > 1:
                        # get the full address
                        if is_eligible_url:
                            # only get eligible url
                            out_links.append(out_link_full)
                else:
                    if is_eligible_url:
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
            print url_id, url, datetime.now()
            return domain_id, url_id

    def addDomain(self, domain):
        if domain not in self.domain_map:
            domain_id = self.domain_num
            self.domain_map[domain] = domain_id

            robot_parser = parseRobot(domain)
            try:
                crawler_delay = robot_parser.get_crawl_delay('*')
            except:
                crawler_delay = None
            self.domain_nodes[domain_id] = {'domain': domain,
                                            'robot': parseRobot(domain),
                                            'delay': crawler_delay}
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
        self.file_html.write(' '.join(['<HEAD>', html_content['title'],
                                        '</HEAD>', '\n']))
        self.file_html.write(' '.join(['<HTTPHEADER>', html_content['http_headers'],
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

                # crawl all the urls in the next wave, and add them as url nodes
                print 'start crawling next wave'
                self.depth += 1
                self.crawlNextLevel(crawl_list, out_links)
            else:
                print 'no new out links'
        # add out link edges of the url remaining in the url_queue
        print 'add remaining link edges'
        self.processRemainEdges()
        print 'running time for crawler', datetime.now() - now
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
                self.crawlURLNextLevel(tmp_url_id, out_links)
                crawler_delay = .5
                domain_id = out_links[tmp_url_id]['domain_id']
                if self.domain_nodes[domain_id]['delay'] is not None:
                    crawler_delay = max(crawler_delay,
                                        self.domain_nodes[domain_id]['delay'])
                if self.url_num > self.MAX_URL_NUM:
                    # stop when meets the requirement
                    return
            # wait at least .5s to avoid visit the same domain too frequently
            time.sleep(crawler_delay)
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
            return robot_parser.is_allowed('*', url)
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

def isValidHTTPHeader(http_headers):
    flag = True
    if 'Content-Type' in http_headers.keys():
        if 'text' not in http_headers['Content-Type']:
            flag = False
    if 'Content-Language' in http_headers.keys():
        if 'en' not in http_headers['Content-Language']:
            flag = False
    return flag

def isEligibleURL(url):
    flag = len(url) < 200 and ' ' not in url and '\n' not in url \
                    and url[:4].lower() == 'http'
    return flag

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

def dictToText(d):
    text = ''
    for key, val in d.iteritems():
        text += ':'.join([key,val]) + '\n'
    return text

def parseRobot(domain):
    robot_url = '/'.join([domain, 'robots.txt'])
    try:
        robot_file = urllib2.urlopen(robot_url).read()
        robot_content = ''
        for l in robot_file.split('\n'):
            if l.replace(' ','') != '':
                robot_content += l + '\n'
        robot_parser = robotexclusionrulesparser.RobotExclusionRulesParser()
        robot_parser.parse(robot_content)
        return robot_parser
    except:
        return None

def getDomain(url): # canonicalized url
    net_split = url.split('://')
    if len(net_split) == 1: # no scheme
        net = net_split[0].split('/')
        return net[0].lower()
    elif len(net_split) == 2:
        scheme, net = net_split
        scheme = 'http' if scheme.lower() == 'https' else scheme.lower()
        net = net.split('/')
        return '://'.join([scheme, net[0].lower()])
    else:
        return ''

def canonicalizeURL(url):
    net_split = url.split('://')
    if len(net_split) == 1:
        scheme, net = '', net_split[0]
    elif len(net_split) == 2:
        scheme, net = net_split
    else:
        return ''
    # remove duplicate slashes and the fragment
    net = net.replace('//','/').split('#')[0].split('/')
    # remove port
    net[0] = net[0].split(':')[0].lower()
    if scheme == '':
        url = '/'.join(net)
    else:
        scheme = 'http' if scheme.lower() == 'https' else scheme.lower()
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
    else:
        relative_url = url
    return relative_url

def isValieURLType(url):
    type_not_covered = ['pdf', 'jpg', 'jpeg', 'gif', 'png', 'svg', 'zip', 'doc',
                        'tif', 'tiff']
    if url.split('.')[-1].lower() in type_not_covered:
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
    print datetime.now()
    seed_url_list = ['http://www.csnchicago.com/bulls',
    'http://www.csnchicago.com/admin/',
    'http://www.csnchicago.com/cubs',
    'https://en.wikipedia.org/wiki/Wikipedia:Benutzersperrung/',
    'http://en.wikipedia.org/wiki/American_Revolutionary_War',
    'http://en.wikipedia.org/wiki/American_Revolution',
    'http://www.revolutionary-war.net/causes-of-the-american-revolution.html',
    'http://www.historycentral.com/Revolt/causes.html',
    'https://www.thoughtco.com/causes-of-the-american-revolution-104860']

    title = 'independ america u.s histor caus revolut reason purpos war authoritarian autocrac capital collaboration colon cronyism despot dictatorship discrimin econom depress econom inequal elector fraud famin fascism feudal imperial militar occup monarch natur disast nepot persecut politic corrupt repress povert totalitarian unemploy'
    crawler = Crawler(seed_url_list, title)
    crawler.crawl()
