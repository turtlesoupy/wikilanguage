import io
import bz2
import gzip
import functools
import tempfile
import time
import itertools
import shelve
import csv
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
    if input_path.endswith(".gz"):
        with open(input_path, mode="rb", buffering=bufsize) as f:
            f = gzip.GzipFile(fileobj=f)
            f = io.BufferedReader(f, buffer_size=bufsize)
            stream = io.TextIOWrapper(f)
            yield stream
    elif input_path.endswith(".bz2"):
        with open(input_path, mode="rb", buffering=bufsize) as f:
            f = bz2.BZ2File(f)
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
            augment_with_pagerank(f.name, in_memory=in_memory), write_path,
        )
    print("All done!")


def write_articles_to_shelf(shelf, input_path, rank_in_memory=True, limit=None):
    start = time.time()
    with tempfile.NamedTemporaryFile() as f:
        store_wikipedia_pages(input_path, f.name, limit=limit)
        for i, page in enumerate(
            augment_with_pagerank(f.name, in_memory=rank_in_memory)
        ):
            if limit and i > limit:
                break

            shelf[page.title] = page

            if i % 100000 == 0:
                delta = time.time() - start
                pps = i / delta
                print(f"Shelf Write: made it to page {i} in {delta:.2f}s {pps:.2f}pps")


def _row_dict_from_line(
    line, wiki_to_article_shelf, parent_finder, whitelisted_wikis=None
):
    entry = WikiDataParser.parse_dump_line(line, whitelisted_wikis=whitelisted_wikis)

    if not entry:
        return None

    row_dict = {
        "concept_id": entry.id,
        "sample_label": entry.sample_label,
        "coord_latitude": entry.sample_coord and entry.sample_coord.latitude,
        "coord_longitude": entry.sample_coord and entry.sample_coord.longitude,
        "coord_altitude": entry.sample_coord and entry.sample_coord.altitude,
        "coord_precision": entry.sample_coord and entry.sample_coord.precision,
        "country_of_origin": entry.country_of_origin,
        "publication_date": entry.publication_date,
    }

    for wiki, shelf in wiki_to_article_shelf.items():
        title = entry.titles_by_wiki.get(wiki)
        if title:
            row_dict[f"{wiki}_title"] = title
            article = shelf.get(title)
            if article:
                row_dict[f"{wiki}_pagerank"] = article.pagerank
            else:
                row_dict[f"{wiki}_pagerank"] = None
        else:
            row_dict[f"{wiki}_title"] = None
            row_dict[f"{wiki}_pagerank"] = None

    parent_concepts = set()
    for c in entry.direct_instance_of:
        parent_finder.all_parents(c, parent_concepts)

    row_dict["direct_instance_of"] = ",".join(entry.direct_instance_of)
    row_dict["instance_of"] = ",".join(parent_concepts)
    return row_dict


def write_csv(
    input_path,
    output_path,
    wiki_to_article_shelf,
    parent_finder,
    whitelisted_wikis=None,
    limit=None,
    concurrency=None,
):
    wikis = set(wiki_to_article_shelf.keys())
    print(wikis)
    if whitelisted_wikis:
        wikis &= set(whitelisted_wikis)
    print(wikis)

    start = time.time()

    with open(output_path, "w") as output:
        writer = csv.DictWriter(
            output,
            delimiter="\t",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            fieldnames=(
                [
                    "concept_id",
                    "sample_label",
                    "coord_latitude",
                    "coord_longitude",
                    "coord_altitude",
                    "coord_precision",
                    "country_of_origin",
                    "publication_date",
                ]
                + list(
                    itertools.chain.from_iterable(
                        (f"{wiki}_title", f"{wiki}_pagerank") for wiki in wikis
                    )
                )
                + ["direct_instance_of", "instance_of",]
            ),
        )

        writer.writeheader()

        with _buffered_stream(input_path) as f:
            print("CSV Write: starting")
            for i, row_dict in enumerate(
                _row_dict_from_line(
                    e,
                    wiki_to_article_shelf=wiki_to_article_shelf,
                    parent_finder=parent_finder,
                    whitelisted_wikis=whitelisted_wikis,
                )
                for e in itertools.islice(f, limit)
            ):
                if row_dict:
                    writer.writerow(row_dict)

                if i % 100000 == 0:
                    delta = time.time() - start
                    pps = i / delta
                    print(
                        f"CSV Write: made it to page {i} in {delta:.2f}s {pps:.2f}pps"
                    )


def wikidata_inheritance_graph(input_path, limit=None):
    with _buffered_stream(input_path) as f:
        return WikiDataParser.inheritance_graph(f, limit=limit)


def store_wikidata_inheritance_graph(input_path, write_path, limit=None):
    inheritance_graph.dump(wikidata_inheritance_graph(input_path))
