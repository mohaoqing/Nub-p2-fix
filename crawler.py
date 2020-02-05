import logging
import re
from urllib.parse import urlparse
from lxml import etree
from collections import Counter
from nltk.corpus import stopwords
import os
import requests


def is_absolute(url):
    return bool(urlparse(url).netloc)  #https://stackoverflow.com/questions/8357098/how-can-i-check-if-a-url-is-absolute-using-python


logger = logging.getLogger(__name__)

#全局变量
mostoutlink_page = ['',0]
longest_page = ['',0]
traps = []


class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """
    ANALYSIS_FILE_NAME = os.path.join(".", "analytics.txt")

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
        self.cnt = Counter()
        self.subcnt = Counter()
        self.url_data = ''
        self.sw = stopwords.words('english')
        self.htmlsw = {"class", "href", "div", "dropdown", "item", "span", "php", "www", "https", "type", "http", "name", "img", "nav", "src", "script", "com", "toggle", "text", "bin", "hidden", "link", "false", "true", "input", "style", "jpg", "alt", "amp", "col", "javascript", "html", "content", "pdf", "png" ,"btn", "icon", "title", "nbsp", "rel", "css" ,"font"}
        self.count = 0

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """

        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)
            self.url_data = url_data
            parsed = urlparse(url)
            if '.' in parsed.netloc:
                sub = parsed.netloc.split(':')[0]
                if 'www' in sub.split('.'):
                    sub = '.'.join(sub.split('.')[1:])
                self.subcnt[sub] += 1

            for next_link in self.extract_next_links(url_data):
                next_link = next_link.strip('/')
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)

        analysis_file = open(self.ANALYSIS_FILE_NAME, "w")

        analysis_file.write("\nWords count")
        for key,value in self.cnt.most_common():
            if key in self.sw:
                del self.cnt[key]
                continue
            if key in self.htmlsw:
                del self.cnt[key]
                continue

        for key,value in self.cnt.most_common(50):
            analysis_file.write('\n\t' + key + " " + str(value))

        analysis_file.write("\nMOSTOUTLINK PAGES")
        analysis_file.write("\n" + mostoutlink_page[0] + " " + str(mostoutlink_page[1]))

        analysis_file.write("\nLONGEST PAGES")
        analysis_file.write("\n" + longest_page[0] + " " + str(longest_page[1]))

        analysis_file.write("\nDOWNLOADED URLS")
        for i in sorted(list(self.frontier.urls_set),reverse=True):
            try:
                analysis_file.write('\n\t' + str(i))
            except UnicodeEncodeError:
                pass

        analysis_file.write("\n\nSUBDOMAINS")
        for key,value in self.subcnt.most_common():
            analysis_file.write('\n\t' + str(key) + " " + str(value))

        analysis_file.write("\n\nTRAPS")
        for i in sorted(traps,reverse=True):
            try:
                analysis_file.write('\n\t' + str(i))
            except UnicodeEncodeError:
                pass

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        if url_data["http_code"] != 200:
            return []
        parsed = urlparse(url_data["url"])

        outputLinks = []  #https://stackoverflow.com/questions/24396406/find-most-common-words-from-a-website-in-python-3 credit to Padraic Cunningham

        doc = etree.HTML(url_data["content"])

        if doc:
            result = doc.xpath('//a/@href')
            ## get subdomain  https://stackoverflow.com/questions/6925825/get-subdomain-from-url-using-python

            for i in result:
                if len(i) > 1:
                    if is_absolute(i):
                        outputLinks.append(i)

                    elif i[0] == '/':
                        outputLinks.append("https://" + parsed.netloc + i)

                    # elif i[0] == '.' and i[1] == '.':
                    #     abs_url = '/'.join(url_data["url"].split('/')[:-2]) + i[2:]
                    #     outputLinks.append(abs_url)
                    #
                    # elif i[0] != '.' and i[0] != '#':
                    #     abs_url = '/'.join(url_data["url"].split('/')[:-1]) + '/' + i
                    #     outputLinks.append(abs_url)

                    elif i[0] != '#':
                        abs_url = '/'.join(url_data["url"].split('/')[:-1]) + '/' + i
                        outputLinks.append(abs_url)

                    else:
                        pass

        ## check if this page has most out links
        if (len(outputLinks)) > mostoutlink_page[1]:
            mostoutlink_page[0] = url_data['url']
            mostoutlink_page[1] = len(outputLinks)

        ##多重实验先决条件， comment out 来验证个例
        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)

        ##开始Invalid检测
        if len(url) > 190:
            traps.append(url + '\n\t\tTraps: Too long for a webpage, might be dynamically created\n')
            return False

        if parsed.scheme not in {"http", "https"}:
            return False

        elif '/calendars/' not in url and ("calendar." in url or "/calendar" in url):
            traps.append(url + '\n\t\tTraps: Calendar included- may create infinite webpages\n')
            return False

        elif len(url.split("/")) > 10 and '..' not in url:
            traps.append(url + "\n\t\tTraps: Recursive paths detected\n")
            return False

        elif (len(url.split('/')) - len(set(url.split('/'))) > 1) and 'http' not in set(url.split('/')) and '..' not in url and '.pdf' not in url:
            traps.append(url + "\n\t\tTraps: Repeat Directories detected\n")
            return False

        elif len(parsed.query.split("&")) > 2 and '..' not in url and '.pdf' not in url:
            traps.append(url + "\n\t\tTraps: Too many queries-may be dynamic page\n")
            return False


        #################d
        #判断完成，证明这个valid之后的操作:

        # put all the words in this page into a counter
        text_string = self.url_data["content"].lower()
        match_pattern = re.findall(r'\b[a-z]{3,15}\b', str(text_string))
        word_count = len(match_pattern)
        for i in match_pattern:
            self.cnt[i] += 1

        # check if this page has most words
        if word_count > longest_page[1] and url.split('.')[-1] != 'com':
            longest_page[0] = url
            longest_page[1] = word_count

        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            return False



