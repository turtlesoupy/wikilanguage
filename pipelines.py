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
