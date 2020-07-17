import io
import bz2
import gzip
import functools
import tempfile
import time
import itertools
import shelve
from pathlib import Path
from tqdm.auto import tqdm
import csv
from pagerank import pagerank_with_percentiles
from wikidata_parser import WikiDataParser
from wikipedia_parser import (
    WikipediaDumpParser,
    WikipediaCanonicalPageResolver,
    WikipediaCanonicalPage,
)
from contextlib import contextmanager
import multiprocessing


def grouper(n, iterable):
    args = [iter(iterable)] * n
    return ([e for e in t if e != None] for t in itertools.zip_longest(*args))


@contextmanager
def buffered_stream(input_path, bufsize_mb=100):
    bufsize = bufsize_mb * 1024 * 1024
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


def buffered_lines_with_progress(input_path, bufsize_mb=100, update_frequency=2000):
    size = Path(input_path).stat().st_size
    bufsize = bufsize_mb * 1024 * 1024
    last_update = 0
    pbar = tqdm(total=size, unit="b", unit_scale=(1.0 / 1024 * 1024))

    if input_path.endswith(".gz"):
        orig_f = open(input_path, mode="rb", buffering=bufsize)
        f = gzip.GzipFile(fileobj=orig_f)
        f = io.BufferedReader(f, buffer_size=bufsize)
        f = io.TextIOWrapper(f)
    elif input_path.endswith(".bz2"):
        orig_f = open(input_path, mode="rb", buffering=bufsize)
        f = bz2.BZ2File(orig_f)
        f = io.BufferedReader(f, buffer_size=bufsize)
        f = io.TextIOWrapper(f)
    else:
        orig_f  = open(input_path, mode="r", buffering=bufsize)
        f = orig_f
    
    try:
        i = 0
        line = f.readline()
        while line:
            line = f.readline()
            yield line
            i += 1

            if i % update_frequency == 1:
                new_location = orig_f.tell()
                pbar.update(new_location - last_update)
                last_update = new_location
    finally:
        f.close()


def store_wikipedia_pages(input_path, write_path, limit=None):
    print("Parsing raw pages")
    with buffered_stream(input_path) as f:
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
        for i, page in enumerate(augment_with_pagerank(f.name, in_memory=rank_in_memory)):
            if limit and i > limit:
                break

            shelf[page.title] = page

            if i % 100000 == 0:
                delta = time.time() - start
                pps = i / delta
                print(f"Shelf Write: made it to page {i} in {delta:.2f}s {pps:.2f}pps")


def _row_dict_from_line(line, wiki_to_article_shelf, parent_finder, whitelisted_wikis=None):
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

    recursive_instance_concepts = set()
    for c in entry.direct_instance_of:
        parent_finder.all_parents(c, recursive_instance_concepts)

    row_dict["direct_instance_of"] = ",".join(entry.direct_instance_of)
    row_dict["recursive_instance_of"] = ",".join(recursive_instance_concepts)

    recursive_subclass_concepts = set()
    for c in entry.direct_subclass_of:
        parent_finder.all_parents(c, recursive_subclass_concepts)
    row_dict["direct_subclass_of"] = list(entry.direct_subclass_of)
    row_dict["recursive_subclass_of"] = list(recursive_subclass_concepts)

    return row_dict


global _pool_shelf
global _pool_wiki_name
def _init_full_wiki_pool(shelf, wiki_name):
    global _pool_shelf 
    global _pool_wiki_name

    _pool_shelf = shelf
    _pool_wiki_name = wiki_name

def _parse_wd_func(line):
    global _pool_wiki_name
    return WikiDataParser.parse_dump_line(line, whitelisted_wikis={_pool_wiki_name})

def _parse_wd_with_shelf_func(line):
    global _pool_shelf
    global _pool_wiki_name

    entry = WikiDataParser.parse_dump_line(line, whitelisted_wikis={_pool_wiki_name})
    wiki_title = entry and entry.titles_by_wiki.get(_pool_wiki_name)
    article = wiki_title and _pool_shelf.get(wiki_title)

    return (entry, wiki_title, article)


def write_full_wiki_csv(
    wikidata_path, output_path, article_shelf, parent_finder, wiki_name, limit=None
):
    whitelisted_wikis = {wiki_name}
    name_to_id = {} 

    chunksize = 256 
    print("Building (name -> concept) mapping")
    pool = multiprocessing.Pool(initializer=_init_full_wiki_pool, initargs=[article_shelf, wiki_name])
    for entry in pool.imap(_parse_wd_func, itertools.islice(buffered_lines_with_progress(wikidata_path), limit), chunksize=chunksize):
        if not entry:
            continue

        wiki_title = entry.titles_by_wiki.get(wiki_name)

        if not wiki_title:
            continue

        name_to_id[wiki_title] = entry.id
    
    print("CSV Write: Starting")
    with open(output_path, "w") as output:
        writer = csv.DictWriter(
            output,
            delimiter="\t",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            fieldnames=(
                [
                    "concept_id",
                    f"{wiki_name}_title",
                    f"{wiki_name}_pagerank",
                    f"{wiki_name}_pagerank_percentile",
                    "coord_latitude",
                    "coord_longitude",
                    "coord_altitude",
                    "coord_precision",
                    "country_of_origin",
                    "publication_date",
                    "direct_instance_of",
                    "recursive_instance_of",
                    "direct_subclass_of",
                    "recursive_subclass_of",
                    f"{wiki_name}_inlinks",
                    f"{wiki_name}_outlinks",
                    f"{wiki_name}_aliases",
                ]
            ),
        )

        writer.writeheader()

        for entry, wiki_title, article in pool.imap(
            _parse_wd_with_shelf_func, 
            itertools.islice(buffered_lines_with_progress(wikidata_path), limit), 
            chunksize=chunksize
        ):
            if not entry:
                continue

            if not wiki_title:
                continue

            if not article:
                continue

            row_dict = {
                "concept_id": entry.id,
                f"{wiki_name}_title": wiki_title, 
                f"{wiki_name}_pagerank": article.pagerank,
                f"{wiki_name}_pagerank_percentile": article.pagerank_percentile,
                "coord_latitude": entry.sample_coord and entry.sample_coord.latitude,
                "coord_longitude": entry.sample_coord and entry.sample_coord.longitude,
                "coord_altitude": entry.sample_coord and entry.sample_coord.altitude,
                "coord_precision": entry.sample_coord and entry.sample_coord.precision,
                "country_of_origin": entry.country_of_origin,
                "publication_date": entry.publication_date,
            }

            row_dict[f"{wiki_name}_inlinks"] = [x for x in [(name_to_id.get(e), cnt) for e, cnt in article.inlinks.items()] if x[0]]
            row_dict[f"{wiki_name}_outlinks"] = [x for x in [(name_to_id.get(e), cnt) for e, cnt in article.links.items()] if x[0]]
            row_dict[f"{wiki_name}_aliases"] = list(article.aliases)


            recursive_instance_concepts = set()
            for c in entry.direct_instance_of:
                parent_finder.all_parents(c, recursive_instance_concepts)
            row_dict["direct_instance_of"] = list(entry.direct_instance_of)
            row_dict["recursive_instance_of"] = list(recursive_instance_concepts)

            recursive_subclass_concepts = set()
            for c in entry.direct_subclass_of:
                parent_finder.all_parents(c, recursive_subclass_concepts)
            row_dict["direct_subclass_of"] = list(entry.direct_subclass_of)
            row_dict["recursive_subclass_of"] = list(recursive_subclass_concepts)

            writer.writerow(row_dict)
            


def write_csv(
    wikidata_path, output_path, wiki_to_article_shelf, parent_finder, whitelisted_wikis=None, limit=None,
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
                + list(itertools.chain.from_iterable((f"{wiki}_title", f"{wiki}_pagerank") for wiki in wikis))
                + ["direct_instance_of", "recursive_instance_of", "direct_subclass_of", "recursive_subclass_of"]
            ),
        )

        writer.writeheader()

        with buffered_stream(wikidata_path) as f:
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
                    print(f"CSV Write: made it to page {i} in {delta:.2f}s {pps:.2f}pps")


def wikidata_inheritance_graph(input_path, limit=None):
    with buffered_stream(input_path) as f:
        return WikiDataParser.inheritance_graph(f, limit=limit)


def store_wikidata_inheritance_graph(input_path, write_path, limit=None):
    inheritance_graph.dump(wikidata_inheritance_graph(input_path))
