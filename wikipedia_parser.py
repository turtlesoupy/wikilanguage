from dataclasses import dataclass
import msgpack
import xml.sax
import wikitextparser as wtp
import multiprocessing
import time
import io
import queue
from collections import namedtuple, Counter
from typing import Set, Optional
import signal

UnparsedRawPage = namedtuple("UnparsedRawPage", ["id", "title", "redirect", "text"])


@dataclass
class ParsedRawPage:
    __slots__ = ["id", "title", "redirect", "links"]

    id: str
    title: str
    redirect: Optional[str]
    links: Counter

    @classmethod
    def dump_collection(cls, pages, path):
        with open(path, "wb") as f:
            for page in pages:
                f.write(page.to_msgpack())

    @classmethod
    def read_collection(cls, path):
        with open(path, "rb") as f:
            unpacker = msgpack.Unpacker(
                f, raw=False, use_list=False, max_map_len=1024 ** 2
            )
            for item in unpacker:
                yield cls.from_msgpack(item)

    @classmethod
    def from_msgpack(cls, item):
        id, title, redirect, links = item
        return cls(id, title, redirect, Counter(links))

    def to_msgpack(self):
        return msgpack.packb(
            (self.id, self.title, self.redirect, self.text), use_bin_type=True
        )


@dataclass
class WikipediaCanonicalPage:
    __slots__ = [
        "id",
        "title",
        "aliases",
        "links",
        "inlinks",
        "pagerank",
        "pagerank_percentile",
    ]

    id: str
    title: str
    aliases: Set
    links: Counter
    inlinks: Counter
    pagerank: Optional[float]
    pagerank_percentile: Optional[float]

    @classmethod
    def dump_collection(cls, pages, path):
        with open(path, "wb") as f:
            for page in pages:
                f.write(page.to_msgpack())

    @classmethod
    def read_collection(cls, path, limit=None, skip_keys=()):
        with open(path, "rb") as f:
            unpacker = msgpack.Unpacker(
                f, raw=False, use_list=False, max_map_len=1024 ** 2
            )
            for i, item in enumerate(unpacker):
                yield WikipediaCanonicalPage.from_msgpack(item, skip_keys=skip_keys)
                if limit and i >= limit:
                    break

    @classmethod
    def from_msgpack(cls, item, skip_keys=()):
        if len(item) == 5:
            (id, title, aliases, links, inlinks) = item
            pagerank = None
            pagerank_percentile = None
        elif len(item) == 7:
            (id, title, aliases, links, inlinks, pagerank, pagerank_percentile) = item
        else:
            raise RuntimeError(
                f"Invalid WikipediaCanonicalPage read from msgpack: {item}"
            )

        return cls(
            id,
            title if "title" not in skip_keys else None,
            set(aliases) if "aliases" not in skip_keys else None,
            Counter(links) if "links" not in skip_keys else None,
            Counter(inlinks) if "inlinks" not in skip_keys else None,
            pagerank if "pagerank" not in skip_keys else None,
            pagerank_percentile if "pagerank_percentile" not in skip_keys else None,
        )

    def to_msgpack(self):
        return msgpack.packb(
            (
                self.id,
                self.title,
                list(self.aliases),
                self.links,
                self.inlinks,
                self.pagerank,
                self.pagerank_percentile,
            ),
            use_bin_type=True,
        )


class WikiXMLHandler(xml.sax.ContentHandler):
    def __init__(self, queue, limit=None):
        super().__init__()

        self.queue = queue
        self.element_count = 0
        self.page_count = 0
        self.in_page = False
        self.limit = limit
        self.revision_text_limit = 100 * 1024 * 1024

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
            raise RuntimeError(f"Encountered element {name} within a title")
        if self.in_revision_text:
            raise RuntimeError(f"Encountered element {name} within revision text")

        if self.in_id:
            raise RuntimeError(f"Encountered element {name} within id")

        if name == "page":
            if self.in_page:
                raise RuntimeError("Recursive page")

            self.in_page = True

        if self.in_page:

            if name == "title":
                if self.page_title:
                    raise RuntimeError("Encountered a second title for the page!")

                self.in_title = True
                self.title_buffer = io.StringIO()

            elif name == "redirect":
                if self.page_redirect:
                    raise RuntimeError(f"Already had a redirect for {self.page_title}")
                if attrs.getLength() != 1:
                    raise RuntimeError(
                        f"More than one redirect attribute for {self.page_title}"
                    )
                self.page_redirect = attrs.getValue("title")

            elif name == "revision":
                if self.seen_page_revision:
                    raise RuntimeError(
                        f"Saw a second page revision for {self.page_title}"
                    )

                self.in_revision = True
            elif self.in_revision and name == "text":
                self.in_revision_text = True
                self.revision_text_buffer = io.StringIO()
                self.revision_text_length = 0
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
                self.revision_text_length = 0
            elif name == "id":
                self.in_id = False
                self.page_id = self.id_buffer.getvalue().strip()
                self.id_buffer = None

    def characters(self, data):
        if self.in_title:
            self.title_buffer.write(data)
        elif self.in_revision_text:
            if self.revision_text_length > self.revision_text_limit:
                print(
                    f"Hit revision text limit for {self.title_buffer.getvalue()}! Skipping"
                )
                return

            self.revision_text_length += len(data)
            self.revision_text_buffer.write(data)
        elif self.in_id:
            self.id_buffer.write(data)

    def handle_page(self):
        self.page_count += 1

        self.queue.put(
            UnparsedRawPage(
                self.page_id, self.page_title, self.page_redirect, self.page_text
            )
        )

        if self.limit and self.page_count >= self.limit:
            raise StopIteration("Stopping")
        elif self.page_count % 10000 == 0:
            delta = time.time() - self.start_time
            print(
                f"Made it to {self.page_title} ({self.page_count}) in {delta}s ({self.page_count / delta})pps"
            )


class TimeoutError(Exception):
    pass


class timeout:
    def __init__(self, seconds=1):
        self.seconds = seconds

    def handle_timeout(self, signum, frame):
        raise TimeoutError("timed out")

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)


class WikipediaDumpParser:
    @classmethod
    def parsed_wikipedia_pages(cls, stream, limit=None, concurrency=None):
        concurrency = concurrency or multiprocessing.cpu_count() * 2

        def unparsed2parsed_worker(reader_queue, writer_queue):
            while True:
                unparsed_page = reader_queue.get()
                if unparsed_page is None:
                    return

                try:
                    # Certain inputs cause infinite spinning while parsing
                    with timeout(seconds=60):
                        parsed = wtp.parse(unparsed_page.text)
                except TimeoutError:
                    print(
                        f"Wikipedia Dump Worker: timed out while parsing '{unparsed_page.title}' ({unparsed_page.id}) of length {len(unparsed_page.text)}"
                    )
                    continue
                page = ParsedRawPage(
                    id=unparsed_page.id,
                    title=unparsed_page.title,
                    redirect=unparsed_page.redirect,
                    links=Counter(e.title.strip() for e in parsed.wikilinks),
                )
                writer_queue.put(page)

        reader_queue = multiprocessing.Queue(concurrency * 10)
        writer_queue = multiprocessing.Queue()
        processes = []
        try:
            for i in range(concurrency):
                p = multiprocessing.Process(
                    target=unparsed2parsed_worker,
                    args=(reader_queue, writer_queue),
                    daemon=True,
                )
                p.start()
                processes.append(p)

            handler = WikiXMLHandler(reader_queue, limit=limit)
            try:
                xml.sax.parse(stream, handler)
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
        except Exception:
            print("Exception raised, terminating subprocesses")
            for p in processes:
                p.terminate()
            raise


class WikipediaCanonicalPageResolver:
    @classmethod
    def resolve_parsed_pages(cls, parsed_pages):
        title_to_wikipedia_page = {}
        redirects = {}
        print("Parsing pages")
        for p in parsed_pages:
            if p.redirect:
                redirects[p.title] = p.redirect
            else:
                title_to_wikipedia_page[p.title] = WikipediaCanonicalPage(
                    id=p.id,
                    title=p.title,
                    aliases=set(),
                    links=p.links.copy(),
                    inlinks=Counter(),
                    pagerank=None,
                    pagerank_percentile=None,
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
        if t > 0:
            print(
                f"Resolved {resolved_count} ({resolved_count / t}) redirects"
                f"with {circle_count} ({circle_count / t}) cycles and"
                f"{unresolvable_count} ({unresolvable_count / t}) unresolvables"
            )

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
                        assert (
                            resolved_link in title_to_wikipedia_page
                        ), f"Bad redirect was formed from '{link}' to '{resolved_link}'!"
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
                            print(
                                f"Sample bad link: '{p.title}' contains unresolved link '{link}'"
                            )
                    continue
                else:
                    good_link_count += 1

            p.links.clear()
            p.links.update(resolved_links)

        t = good_link_count + bad_link_count
        if t > 0:
            print(
                f"Found {good_link_count} ({good_link_count / t}) good links "
                f"and {bad_link_count} ({bad_link_count / t}) bad links "
                f"and {file_count} ({file_count / t}) file links"
            )

        while True:
            try:
                _, val = title_to_wikipedia_page.popitem()
            except KeyError:
                break

            yield val
