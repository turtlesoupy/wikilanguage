import io
import bz2
import gzip
import tempfile
import time
import itertools
from pagerank import pagerank_with_percentiles
from wikidata_parser import WikiDataParser
from wikipedia_parser import (
    WikipediaDumpParser,
    WikipediaCanonicalPageResolver,
    WikipediaCanonicalPage,
)
from contextlib import contextmanager

def grouper(n, iterable):
    args = [iter(iterable)] * n
    return ([e for e in t if e != None] for t in itertools.zip_longest(*args))

@contextmanager
def _buffered_stream(input_path):
    bufsize = 100 * 1024 * 1024
    if input_path.endswith(".bz2"):
        with open(input_path, mode="rb", buffering=bufsize) as f:
            f = bz2.BZ2File(f)
            f = io.BufferedReader(f, buffer_size=bufsize)
            stream = io.TextIOWrapper(f)
            yield stream
    elif input_path.endswith(".gz"):
        with open(input_path, mode="rb", buffering=bufsize) as f:
            f = gzip.GzipFile(fileobj=f)
            f = io.BufferedReader(f, buffer_size=bufsize)
            stream = io.TextIOWrapper(f)
            yield stream
    else:
        with open(input_path, mode="r", buffering=bufsize) as f:
            yield f


def store_wikipedia_pages(input_path, write_path, limit=None):
    print("Parsing raw pages")
    with _buffered_stream(input_path) as f:
        raw_pages = WikipediaDumpParser.parsed_wikipedia_pages(f, limit=limit)
    print("Parsed! Resolving links")
    wiki_pages = list(WikipediaCanonicalPageResolver.resolve_parsed_pages(raw_pages))
    wiki_pages.sort(key=lambda x: x.title)
    print("Writing results")
    WikipediaCanonicalPage.dump_collection(wiki_pages, write_path)


def augment_with_pagerank(canonical_file, in_memory=True):
    if in_memory:
        c = list(WikipediaCanonicalPage.read_collection(canonical_file))

        def loader():
            return c

    else:

        def loader():
            return WikipediaCanonicalPage.read_collection(canonical_file)

    for page, pr, pr_percentile in pagerank_with_percentiles(loader):
        page.pagerank = pr
        page.pagerank_percentile = pr_percentile
        yield page

def store_wiki_with_pagerank(input_path, write_path, limit=None, in_memory=True):
    with tempfile.NamedTemporaryFile() as f:
        store_wikipedia_pages(input_path, f.name, limit=limit)
        WikipediaCanonicalPage.dump_collection(
            augment_with_pagerank(f.name, in_memory=in_memory),
            write_path,
        )
    print("All done!")
    
def write_articles_into_db(db, input_path, wiki_name, rank_in_memory=True, db_batch_size=10000, limit=None):
    start = time.time()
    with tempfile.NamedTemporaryFile() as f:
        store_wikipedia_pages(input_path, f.name, limit=limit)
        for i, pages in enumerate(grouper(db_batch_size, augment_with_pagerank(f.name, in_memory=rank_in_memory))):
            with db.con as cur:
                cur.executemany(
                    """
                    INSERT INTO articles(wiki, title, id, pagerank, pagerank_percentile) 
                    VALUES(?, ?, ?, ?, ?)
                    """,
                    (
                        (
                            wiki_name,
                            p.title, 
                            p.id,
                            p.pagerank,
                            p.pagerank_percentile,
                        )
                        for p in pages
                    )
                )
                page_num = (i + 1) * db_batch_size
                if page_num % 100000 == 0:
                    delta = time.time() - start
                    pps = page_num / delta
                    print(f"DB Write: made it to page {page_num} in {delta:.2f}s {pps:.2f}pps") 
        print("Finished writing!")
             
            
def default_instance_map(inheritance_graph):
    def d(the_id):
        return {e for e in inheritance_graph.descendent_ids(the_id)}

    return {
        "city": d("Q2095"),
        "food": d("Q515"),
        "human": d("Q5"),
        "country": d("Q6256"),
        "year": d("Q577"),
        "tourist attraction": d("Q570116"),
        "archaeological site": d("Q839954"),
        "temple": d("Q44539"),
        "job": d("Q192581"),
        "higher_education": d("Q38723"),
        "anime film": d("Q20650540"),
        "film": d("Q11424"),
        "building": d("Q41176"),
        "mountain": d("Q8502"),
        "trail": d("Q628179"),
        "event": d("Q1656682"),
        "television series": d("Q5398426"),
        "website": d("Q35127"),
        "language": d("Q34770"),
        "human-geographic": d("Q15642541"),
        "political-territorial-entity": d("Q1048835"),
    }


def store_wikidata(
    input_path, write_path, instance_map={}, whitelisted_wikis=None, limit=None
):
    with _buffered_stream(input_path) as f:
        wikidata = WikiDataParser.parse_dump(
            f, whitelisted_wikis=whitelisted_wikis, instance_map=instance_map
        )
    wikidata.dump(write_path)


def store_wikidata_inheritance_graph(input_path, write_path, limit=None):
    with _buffered_stream(input_path) as f:
        inheritance_graph = WikiDataParser.inheritance_graph(f)

    inheritance_graph.dump(write_path)