import argparse
import re
import tldextract
from urlparse import urlparse 
from twisted.internet import reactor, task
from twisted.web.client import getPage
from bs4 import BeautifulSoup

class EmailScraper():
    # URLs already visited  
    already_visited = set()

    # Emails recognized so far
    emails_extracted = set()

    # Home page that we're scraping, including protocol and tld, used to compose relative links
    base_url = None

    # Domain name that needs to match the links to prevent the crawler from escaping 
    domain_name = None

    # Subdomain (if there is one) that the crawler is allowed to traverse
    subdomain_name = None

    # Number of requests Twisted is currently waiting on 
    deferred_count = 0
        
    # Max number of active connections 
    # Set this through trial and error - some sites will throttle you 
    # Your OS may also get upset about too much IO, so if you get weird errors try lowering this first
    concurrent_connection_limit = 25 

    # Queue for urls when the active connection limit is reached
    to_scrape_queue = []

    def __init__(self, domain):
        self.to_scrape_queue.append(domain)

        self.base_url = domain
        self.domain_name = tldextract.extract(domain).domain 
        self.subdomain_name = tldextract.extract(domain).subdomain

        ctc = task.LoopingCall(self.check_termination_condition)
        ctc.start(1)

        mc = task.LoopingCall(self.manage_crawlers)
        mc.start(1)
        
        self.reactor = reactor
        self.reactor.run()

    def check_termination_condition(self):
        # When there are no more URLs waiting to be scraped, and no more deferreds awaiting callbck, the program is done
        if len(self.to_scrape_queue) == 0 and self.deferred_count == 0:
            self.reactor.stop()
            for email in self.emails_extracted:
                print email

    # Keeps the number of active connections managed
    def manage_crawlers(self):
        while len(self.to_scrape_queue) and self.deferred_count < self.concurrent_connection_limit:
            next_page = self.to_scrape_queue.pop()

            # Something deep in getPage breaks when it's passed certain(?) unicode characters, so lets get rid of them and hope for the best in that case 
            d = getPage(next_page.encode('ascii', 'ignore'), timeout=5)
            d.addCallback(self.extract_and_crawl)
            d.addErrback(self.failure)
            self.deferred_count += 1

    def failure(self, res):
        self.deferred_count -= 1

    # Callback for select loop
    # Pulls out email addresses and URLs on the same domain 
    def extract_and_crawl(self, res):
        # Extract emails
        # First pass - simple regular expression
        emails = re.findall(r'[A-Za-z0-9.]+@[A-Za-z0-9-]+\.[A-Za-z]+', res)
        for email in emails:
            self.emails_extracted.add(email)

        # lxml is supposedly much faster than the parser that comes with bs4, so let's use it instead
        soup = BeautifulSoup(res, 'lxml') 
            
        # Extract links
        anchors = soup.find_all('a', href=True)
        urls = [tag['href'] for tag in anchors]

        # Extract more emails 
        # Second pass - parse anchors with mailto: in them  
        emails =[tag['href'].replace('mailto:','') for tag in anchors if 'mailto:' in tag['href']]
        for email in emails:
            self.emails_extracted.add(email)

        # Handle relative links 
        relative_links = [self.base_url+u for u in urls if 'http' not in u[:4]]
        absolute_links = [u for u in urls if 'http' in u[:4]]
        to_scrape = relative_links+absolute_links

        # Remove URLs with the wrong subdomain check domain, check already visited 
        to_scrape = [u for u in to_scrape if tldextract.extract(u).domain == self.domain_name and \
                     (tldextract.extract(u).subdomain == self.subdomain_name or tldextract.extract(u).subdomain == 'www')]
        to_scrape = [u for u in to_scrape if u not in self.already_visited] 
        
        # Dedeuplicate and add to the queue
        for link in set(to_scrape):
            self.already_visited.add(link)
            self.to_scrape_queue.append(link)
        
        # Finally, decrement deferred_count since this callback is complete
        self.deferred_count -= 1
        return res
                
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('domain')
    args = parser.parse_args()
    
    # Add an http if the user didn't 
    dom = args.domain
    if len(dom) < 7 or 'http' not in dom[:4]:
        dom = 'http://'+dom

    EmailScraper(dom)    

if __name__ == "__main__":
    main()
