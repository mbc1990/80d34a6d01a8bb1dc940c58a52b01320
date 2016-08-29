import argparse
import re
import tldextract
from urlparse import urlparse 
from twisted.internet import reactor, task
from bs4 import BeautifulSoup
from twisted.web.client import getPage

# URLs already visited  
already_visited = set()

# Emails recognized so far
emails_extracted = set()

# Home page that we're scraping, including protocol and tld, used to compose relative links
base_url = None

# Domain name that needs to match the links to prevent the crawler from escaping 
domain_name = None

# Number of requests Twisted is currently waiting on 
deferred_count = 0
    
# Max number of active connections 
active_connection_limit = 10 

# Queue for urls when the active connection limit is reached
to_scrape_queue = []

def main():
    global deferred_count
    global base_url
    global domain_name
    global to_scrape_queue

    parser = argparse.ArgumentParser()
    parser.add_argument('domain')
    args = parser.parse_args()

    to_scrape_queue.append(args.domain)

    base_url = args.domain 
    domain_name = tldextract.extract(args.domain).domain 
    
    ctc = task.LoopingCall(check_termination_condition)
    ctc.start(1)

    mc = task.LoopingCall(manage_crawlers)
    mc.start(1)

    reactor.run()

def check_termination_condition():
    global to_scrape_queue 
    global deferred_count 
    global emails_extracted

    # When there are no more URLs waiting to be scraped, and no more deferreds waiting to be executed, the program is done
    if len(to_scrape_queue) == 0 and deferred_count == 0:
        reactor.stop()
        for email in emails_extracted:
            print email

# Keeps the number of active connections managed
def manage_crawlers():
    global active_connection_limit
    global to_scrape_queue
    global deferred_count
    
    while len(to_scrape_queue) and deferred_count < active_connection_limit:
        next_page = to_scrape_queue.pop()

        d = getPage(next_page, timeout=5)
        d.addCallback(extract_and_crawl, url=next_page)
        d.addErrback(failure)

        deferred_count += 1

def failure(res):
    global deferred_count
    deferred_count -= 1

# Callback for select loop
# Pulls out email addresses and URLs on the same domain 
def extract_and_crawl(res, url='URL PARAM DISABLED'):
    global deferred_count
    global already_visited
    global base_url
    global domain_name
    global emails_extracted
    global to_scrape_queue

    # Extract emails
    # This regex is a simplified version of a more complex one I found online
    # I think recognizing email addresses with regular expressions may be a fool's errand 
    # So I have left the implementation of one conforming with all the relevant RFCs (and unicode characters!) as an exercize to the reader
    # TODO: Could parse mailto: if we're optimistic about sites conforming to that 
    emails = re.findall(r"[A-Za-z0-9.]+@[A-Za-z0-9-]+\.[A-Za-z]{2,10}", res)
    for email in emails:
        emails_extracted.add(email)

    # lxml is supposedly much faster than the parser that comes with bs4, so let's use it
    soup = BeautifulSoup(res, 'lxml') 
        
    # Extract links
    anchors = soup.find_all('a', href=True)
    urls = [tag['href'] for tag in anchors]

    # Handle relative links 
    relative_links = [base_url+u for u in urls if 'http' not in u[:4]]
    absolute_links = [u for u in urls if 'http' in u[:4]]
    to_scrape = relative_links+absolute_links
        
    # Remove URLs with subdomains, check domain, check already visited 
    to_scrape = [u for u in to_scrape if tldextract.extract(u).domain == domain_name and (tldextract.extract(u).subdomain == ''\
                                                                                          or tldextract.extract(u).subdomain == 'www')]
    to_scrape = [u for u in to_scrape if u not in already_visited] 
    
    # Dedeuplicate and add to the queue
    for link in set(to_scrape):
        already_visited.add(link)
        to_scrape_queue.append(link)
    
    # Finally, decrement deferred_count since this callback is complete
    deferred_count -= 1
    return res

if __name__ == "__main__":
    main()
