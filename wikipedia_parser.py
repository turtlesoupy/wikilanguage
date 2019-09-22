import msgpack
import json
import pickle
import xml.sax
import wikitextparser as wtp
import multiprocessing 
import sys
import time
import io
import queue
from sys import getsizeof
from collections import namedtuple, Counter, defaultdict, OrderedDict

UnparsedRawPage = namedtuple("UnparsedRawPage", ["id", "title", "redirect", "text"])

class WikiXMLHandler(xml.sax.ContentHandler):
    def __init__(self, queue, limit=None):
        super().__init__()
        
        self.queue = queue
        self.element_count = 0
        self.page_count = 0
        self.in_page = False
        self.limit = limit
        
        # Per page
        self.in_title = False
        self.title_buffer = None
        self.page_title = None
        self.page_text = None
        self.page_redirect = None
        
        self.seen_page_revision = False
        self.in_revision = False
        self.in_revision_text = False
        
        self.in_id = False
        self.id_buffer = None
        
        self.page_id = None 
        self.start_time = time.time()

         
    def startElement(self, name, attrs):
        self.element_count += 1
        
            
        if self.in_title:
            raise RuntimeException(f"Encountered element {name} within a title")
        if self.in_revision_text:
            raise RuntimeException(f"Encountered element {name} within revision text")
            
        if self.in_id:
            raise RuntimeException(f"Encountered element {name} within id")
        
        if name == "page":
            if self.in_page:
                raise RuntimeException("Recursive page")

            self.in_page = True
            
        if self.in_page:
            
            if name == "title":
                if self.page_title:
                    raise RuntimeException("Encountered a second title for the page!")

                self.in_title = True
                self.title_buffer = io.StringIO()

            elif name == "redirect":
                if self.page_redirect:
                    raise RuntimeException(f"Already had a redirect for {self.page_title}")
                if attrs.getLength() != 1:
                    raise RuntimeException(f"More than one redirect attribute for {self.page_title}")
                self.page_redirect = attrs.getValue("title")
                

            elif name == "revision":
                if self.seen_page_revision:
                    raise RuntimeException(f"Saw a second page revision for {self.page_title}")

                self.in_revision = True
            elif self.in_revision and name == "text":
                self.in_revision_text = True
                self.revision_text_buffer = io.StringIO()
            elif name == "id":
                self.in_id = True
                self.id_buffer = io.StringIO()

    def endElement(self, name):
        if name == "page":
            self.handle_page()
            
            self.in_page = False
            self.page_title = None
            self.page_text = None
            self.seen_page_revision = False
            self.page_redirect = None
            
        if self.in_page:
            if name == "title":
                self.in_title = False
                self.page_title = self.title_buffer.getvalue()
                self.title_buffer = None
            elif name == "redirect":
                pass
            elif name == "revision":
                self.in_revision = False
                self.seen_page_revision = True
            elif self.in_revision and name == "text":
                self.in_revision_text = False
                self.page_text = self.revision_text_buffer.getvalue()
                self.revision_text_buffer = None
            elif name == "id":
                self.in_id = False
                self.page_id = self.id_buffer.getvalue().strip()
                self.id_buffer = None
            

        
    def characters(self, data):
        if self.in_title:
            self.title_buffer.write(data)
        elif self.in_revision_text:
            self.revision_text_buffer.write(data)
        elif self.in_id:
            self.id_buffer.write(data)
            
            
    def handle_page(self):
        self.page_count += 1
        
        self.queue.put(
            UnparsedRawPage(self.page_id, self.page_title, self.page_redirect, self.page_text)
        )
        
    
        if self.limit and self.page_count >= self.limit:
            raise StopIteration("Stopping")
        elif self.page_count % 10000 == 0:
            delta = time.time() - self.start_time
            print(f"Made it to {self.page_title} ({self.page_count}) in {delta}s ({self.page_count / delta})pps")


class ParsedRawPage(namedtuple("ParsedRawPage", ["id", "title", "redirect", "links"])):
    @classmethod
    def dump_pages(cls, pages, path):
        with open(path, 'wb') as f:
            for page in pages:
                f.write(msgpack.packb(page, use_bin_type=True))

    @classmethod
    def read_pages(cls, path):
        with open(path, 'rb') as f:
            unpacker = msgpack.Unpacker(f, raw=False, use_list=False, max_map_len=1024**2)
            for (id, title, redirect, links) in unpacker:
                yield ParsedRawPage(id, title, redirect, Counter(links))

    @classmethod
    def parsed_wikipedia_pages(cls, filename, limit=None, concurrency=None):
	concurrency = concurrency or multiprocessing.cpu_count() * 2
	def unparsed2parsed_worker(reader_queue, writer_queue): 
	    while True:
		unparsed_page = reader_queue.get()
		if unparsed_page is None:
		    return

		parsed = wtp.parse(unparsed_page.text)    
		page = cls(unparsed_page.id, unparsed_page.title, unparsed_page.redirect, Counter(
		    e.title.strip() for e in parsed.wikilinks
		))
		writer_queue.put(page)

	reader_queue = multiprocessing.Queue(concurrency * 10)
	writer_queue = multiprocessing.Queue()
	processes = []
	try:
	    for i in range(concurrency):
		p = multiprocessing.Process(
		    target=unparsed2parsed_worker, 
		    args=(reader_queue, writer_queue), 
		    daemon=True
		)
		p.start()
		processes.append(p)

	    handler = WikiXMLHandler(reader_queue, limit=limit)
	    try:
		xml.sax.parse(filename, handler)
	    except StopIteration:
		pass

	    for p in processes:
		reader_queue.put(None)


	    pages = []
	    while True:
		try:
		    pages.append(writer_queue.get(False))
		except queue.Empty:
		    if any(p.is_alive() for p in processes):
			continue
		    else:
			break

	    return pages
	except:
	    print("Exception raised, terminating subprocesses")
	    for p in processes:
		p.terminate()
	    raise

class WikipediaPage(namedtuple("WikipediaPage", ["id", "title", "aliases", "links", "inlinks"])):
    @classmethod
    def dump_pages(cls, pages, path):
        with open(path, 'wb') as f:
            for (id, title, aliases, links, inlinks) in pages:
                f.write(msgpack.packb((
                    id, title, list(aliases), links, inlinks
                ), use_bin_type=True))

    @classmethod
    def read_pages(cls, path):
        with open(path, 'rb') as f:
            unpacker = msgpack.Unpacker(f, raw=False, use_list=False, max_map_len=1024**2)
            for (id, title, aliases, links, inlinks) in unpacker:
                yield WikipediaPage(id, title, set(aliases), Counter(links), Counter(inlinks))
                
    @classmethod
    def resolve_parsed_pages(cls, parsed_pages):
        title_to_wikipedia_page = {}
        redirects = {}
        print("Parsing pages")
        for p in parsed_pages:
            if p.redirect:
                redirects[p.title] = p.redirect
            else:
                title_to_wikipedia_page[p.title] = WikipediaPage(
                    id=p.id,
                    title=p.title,
                    aliases=set(),
                    links=p.links.copy(),
                    inlinks=Counter(),
                )
        
        # Making redirect chainer
        print("Creating aliases")
        circle_count = 0
        unresolvable_count = 0
        resolved_count = 0
        for title, redirect in redirects.items():
            resolution_pointer = redirect
            seen_pages = set()
            while True:
                if resolution_pointer in seen_pages:
                    # print(f"WARN: Circular loop with {seen_pages}")
                    redirects[title] = None
                    circle_count += 1
                    break
                
                seen_pages.add(resolution_pointer)
                
                if resolution_pointer in title_to_wikipedia_page:
                    redirects[title] = resolution_pointer
                    title_to_wikipedia_page[resolution_pointer].aliases.add(title)
                    resolved_count += 1
                    break
                elif resolution_pointer in redirects:
                    resolution_pointer = redirects[resolution_pointer]
                else:
                    # print(f"WARN: '{title}' contains unresolvable redirect '{redirect}'")
                    redirects[title] = None
                    unresolvable_count += 1
                    break
        
        t = circle_count + unresolvable_count + resolved_count
        assert t == len(redirects), "Not tautology with all redirects"
        print(f"Resolved {resolved_count} ({resolved_count / t}) redirects with {circle_count} ({circle_count / t}) cycles and {unresolvable_count} ({unresolvable_count / t}) unresolvables")
                   
        print("Resolving deepest links")
        bad_link_count = 0
        good_link_count = 0
        file_count = 0
        # Resolve links to deepest page
        for p in title_to_wikipedia_page.values():
            resolved_links = Counter()
            for raw_link, count in p.links.items():
                resolved = False
                # Wikipedia links occasionally upper case first letter
                for link in (raw_link, raw_link.capitalize()):    
                    if link in redirects and redirects[link] is not None:
                        resolved_link = redirects[link]
                        assert resolved_link in title_to_wikipedia_page, f"Bad redirect was formed from '{link}' to '{resolved_link}'!"
                        resolved_links[resolved_link] += count
                        title_to_wikipedia_page[resolved_link].inlinks[p.title] += count
                        resolved = True
                        break
                    elif link in title_to_wikipedia_page:
                        resolved_links[link] += count
                        title_to_wikipedia_page[link].inlinks[p.title] += count
                        resolved = True
                        break
                        
                if not resolved:
                    # print(f"WARN: '{p.title}' contains unresolved link '{link}'")
                    if raw_link.startswith("File:") or raw_link.startswith("Image:"):
                        file_count += count
                    else:
                        bad_link_count += count
                        if bad_link_count % 100000 == 0:
                            print(f"Sample bad link: '{p.title}' contains unresolved link '{link}'")
                    continue        
                else:
                    good_link_count += 1
             
            p.links.clear()
            p.links.update(resolved_links)
            
        t = good_link_count + bad_link_count
        print(f"Found {good_link_count} ({good_link_count / t}) good links and {bad_link_count} ({bad_link_count / t}) bad links and {file_count} ({file_count / t}) file links")
                

        while True:
            try:
                _, val = title_to_wikipedia_page.popitem()
            except KeyError:
                break
                
            yield val
