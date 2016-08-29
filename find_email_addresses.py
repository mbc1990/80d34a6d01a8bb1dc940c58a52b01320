import argparse
import re

from twisted.internet import reactor, task
from bs4 import BeautifulSoup
from twisted.web.client import getPage

# URLs already visited  
already_visited = set()

# Emails recognized 
emails_extracted = set()

# Domain we're scraping, prevents the crawler from escaping
valid_domain = None

# Outstanding callbacks
deferred_count = 0
    
# Max number of active connections 
active_connection_limit = 5

# Queue for urls when the active connection limit is reached
to_scrape_queue = []

# For debugging
active_scrapers = []

# TODO: Email parsing
def main():
    global deferred_count
    global valid_domain
    global to_scrape_queue

    parser = argparse.ArgumentParser()
    parser.add_argument('domain')
    args = parser.parse_args()
            
    to_scrape_queue.append(args.domain)
    
    # TODO: Parse the actual domain out of this in case a page elsewhere is linked
    valid_domain = args.domain 
    
    ctc = task.LoopingCall(check_termination_condition)
    ctc.start(1)

    mc = task.LoopingCall(manage_crawlers)
    mc.start(1)

    reactor.run()

def check_termination_condition():
    global to_scrape_queue 
    global deferred_count 

    # When there are no more URLs waiting to be scraped, and no more deferreds waiting to be executed, the program is done
    if len(to_scrape_queue) == 0 and deferred_count == 0:
        reactor.stop()

# Keeps the number of active connections managed
def manage_crawlers():
    global active_connection_limit
    global to_scrape_queue
    global deferred_count
    global active_scrapers
    
    # Debug
    global already_visited
    print str(len(to_scrape_queue))+" pages in queue"
    print str(deferred_count)+" active requests"
    print str(len(already_visited))+" pages visited"
    while len(to_scrape_queue) and deferred_count < active_connection_limit:
        next_page = to_scrape_queue.pop()
        print "Extracting: "+next_page
        active_scrapers.append(next_page)

        d = getPage(next_page, timeout=5)
        d.addCallback(extract_and_crawl, url=next_page)
        d.addErrback(failure)

        deferred_count += 1

def failure(res):
    global deferred_count
    print 'failure'+str(res)
    deferred_count -= 1

# Callback for select loop
# Pulls out email addresses and URLs on the same domain 
def extract_and_crawl(res, url='URL PARAM DISABLED'):
    global deferred_count
    global already_visited
    global valid_domain
    global emails_extracted
    global to_scrape_queue
    global active_scrapers

    # lxml is supposedly much faster than the parser that comes with bs4, so let's use it
    soup = BeautifulSoup(res, 'lxml') 
    # TODO: Extract emails
        
    # Extract links
    anchors = soup.find_all('a', href=True)
    urls = [tag['href'] for tag in anchors]

    # Handle relative links 
    relative_links = [valid_domain+u for u in urls if 'http' not in u[:4]]
    absolute_links = [u for u in urls if 'http' in u[:4]]
    to_scrape = relative_links+absolute_links
    to_scrape = [u for u in to_scrape if u not in already_visited and valid_domain in u] 
    
    for link in to_scrape:
        already_visited.add(link)
        to_scrape_queue.append(link)
    
    # Finally, decrement deferred_count since this callback is complete
    deferred_count -= 1
    active_scrapers.remove(url)
    return res

if __name__ == "__main__":
    main()
