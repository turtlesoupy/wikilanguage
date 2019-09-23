import gzip
from pagerank import pagerank_with_percentiles
from wikidata_parser import WikiDataParser
from wikipedia_parser import WikipediaDumpParser, WikipediaCanonicalPageResolver, WikipediaCanonicalPage


def store_wikidata_dump(input_path, write_path, whitelisted_wikis=None):
    WikiDataParser.dump(WikiDataParser.parse_dump(input_path, whitelisted_wikis=whitelisted_wikis), write_path)


def store_wikipedia_pages(input_path, write_path, limit=None):
    print("Parsing raw pages")
    raw_pages = WikipediaDumpParser.parsed_wikipedia_pages(input_path, limit=limit)
    print("Parsed! Resolving links")
    wiki_pages = list(WikipediaCanonicalPageResolver.resolve_parsed_pages(raw_pages))
    wiki_pages.sort(key=lambda x: x.title)
    print("Writing results")
    WikipediaCanonicalPage.dump_collection(wiki_pages, write_path)


def store_wikidata(input_path, write_path, whitelisted_wikis=None, limit=None):
    with gzip.open("/mnt/cold/Projects/data/latest-all.json.gz", mode="rt") as f:
        wikidata = WikiDataParser.parse_dump(
            f,
            whitelisted_wikis=whitelisted_wikis,
        )
    wikidata.dump(write_path)


def augment_with_pagerank(canonical_file, write_path, in_memory=True):
    if in_memory:
        c = WikipediaCanonicalPage.read_collection(canonical_file)
        def loader(): return c
    else:
        def loader(): return WikipediaCanonicalPage.read_collection(canonical_file)

    def yielder():
        for page, pr, pr_percentile in pagerank_with_percentiles(loader):
            page.pagerank = pr
            page.pagerank_percentile = pr_percentile
            yield page

    WikipediaCanonicalPage.dump_collection(yielder(), write_path)
    print("All done!")
