import io
import bz2
import logging
import ujson
import gzip
import tempfile
import itertools
from pathlib import Path
from tqdm.auto import tqdm
import sys
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

from typing import Iterable

logger = logging.getLogger(__name__)


def grouper(n, iterable):
    args = [iter(iterable)] * n
    return ([e for e in t if e is not None] for t in itertools.zip_longest(*args))


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
    input_path = str(input_path)
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
        orig_f = open(input_path, mode="r", buffering=bufsize)
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
    logger.info("store_wikipedia_pages: Parsing raw pages")
    with buffered_stream(input_path) as f:
        raw_pages = WikipediaDumpParser.parsed_wikipedia_pages(f, limit=limit)
    logger.info("store_wikipedia_pages: Parsed! Resolving links")
    wiki_pages = list(WikipediaCanonicalPageResolver.resolve_parsed_pages(raw_pages))
    wiki_pages.sort(key=lambda x: x.title)
    logger.info("store_wikipedia_pages: Writing results")
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


def write_articles_to_shelf(shelf, input_path, rank_in_memory=True, limit=None):
    with tempfile.NamedTemporaryFile() as f:
        logger.info("write_articles_to_shelf: storing wikipedia pages")
        store_wikipedia_pages(input_path, f.name, limit=limit)
        logger.info("write_articles_to_shelf: augmenting with page rank")
        for i, page in enumerate(
            tqdm(augment_with_pagerank(f.name, in_memory=rank_in_memory))
        ):
            shelf[page.title] = page


def write_aliases_to_shelf(articles: Iterable[WikipediaCanonicalPage], shelf):
    for article in articles:
        shelf[article.name] = article.name
        for alias in article.aliases:
            shelf[alias] = article.title


global _pool_shelf
global _pool_alias_map
global _pool_wiki_name


def _init_full_wiki_pool(shelf, alias_map, wiki_name):
    global _pool_shelf
    global _pool_alias_map
    global _pool_wiki_name

    _pool_shelf = shelf
    _pool_alias_map = alias_map
    _pool_wiki_name = wiki_name


def _parse_wd_with_shelf_func(line):
    global _pool_shelf
    global _pool_alias_map
    global _pool_wiki_name

    entry = WikiDataParser.parse_dump_line(line, whitelisted_wikis={_pool_wiki_name})
    wiki_title = entry and entry.titles_by_wiki.get(_pool_wiki_name)
    article = None
    aliased_from = None

    if wiki_title:
        article = _pool_shelf.get(wiki_title)
        if not article:
            alias = _pool_alias_map.get(wiki_title)
            if alias:
                article = _pool_shelf.get(alias)
                if article:
                    aliased_from = wiki_title
                    wiki_title = alias

    return (entry, wiki_title, article, aliased_from)


def build_alias_map(article_shelf, limit=None):
    return dict(itertools.chain.from_iterable(
        (
            ((alias, article_name) for alias in article.aliases)
            for article_name, article in itertools.islice(article_shelf.items(), limit)
        )
    ))


def write_full_wiki_csv(
    wikidata_path, output_path, article_shelf, parent_finder, wiki_name, alias_map, limit=None
):
    chunksize = 256
    field_names = [
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
    csv_format_params = dict(
        delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL, fieldnames=field_names
    )
    csv.field_size_limit(sys.maxsize)
    num_considered = 0
    num_empty = 0
    num_no_title = 0
    num_missing_article = 0
    num_aliases = 0

    logging.info("Writing TSV")
    with multiprocessing.Pool(
        initializer=_init_full_wiki_pool, initargs=[article_shelf, alias_map, wiki_name]
    ) as pool, tempfile.TemporaryDirectory() as temp_dir:
        tmp_intermediate_path = Path(temp_dir) / "intermediate.csv"
        with open(tmp_intermediate_path, "w") as fw:
            intermediate_writer = csv.DictWriter(fw, **csv_format_params)
            intermediate_writer.writeheader()

            name_to_id = {}
            for entry, wiki_title, article, aliased_from in pool.imap(
                _parse_wd_with_shelf_func,
                itertools.islice(buffered_lines_with_progress(wikidata_path), limit),
                chunksize=chunksize,
            ):
                num_considered += 1
                if not entry:
                    num_empty += 1
                    continue

                if not wiki_title:
                    num_no_title += 1
                    continue

                if not article:
                    num_missing_article += 1
                    continue

                if aliased_from:
                    num_aliases += 1

                name_to_id[wiki_title] = entry.id

                row_dict = {
                    "concept_id": entry.id,
                    f"{wiki_name}_title": wiki_title,
                    f"{wiki_name}_pagerank": article.pagerank,
                    f"{wiki_name}_pagerank_percentile": article.pagerank_percentile,
                    "coord_latitude": entry.sample_coord
                    and entry.sample_coord.latitude,
                    "coord_longitude": entry.sample_coord
                    and entry.sample_coord.longitude,
                    "coord_altitude": entry.sample_coord
                    and entry.sample_coord.altitude,
                    "coord_precision": entry.sample_coord
                    and entry.sample_coord.precision,
                    "country_of_origin": entry.country_of_origin,
                    "publication_date": entry.publication_date,
                }

                row_dict[f"{wiki_name}_inlinks"] = ujson.dumps(
                    list(article.inlinks.items())
                )
                row_dict[f"{wiki_name}_outlinks"] = ujson.dumps(
                    list(article.links.items())
                )
                row_dict[f"{wiki_name}_aliases"] = list(article.aliases)

                recursive_instance_concepts = set()
                for c in entry.direct_instance_of:
                    parent_finder.all_parents(c, recursive_instance_concepts)
                row_dict["direct_instance_of"] = ujson.dumps(
                    list(entry.direct_instance_of)
                )
                row_dict["recursive_instance_of"] = ujson.dumps(
                    list(recursive_instance_concepts)
                )

                recursive_subclass_concepts = set()
                for c in entry.direct_subclass_of:
                    parent_finder.all_parents(c, recursive_subclass_concepts)
                row_dict["direct_subclass_of"] = ujson.dumps(
                    list(entry.direct_subclass_of)
                )
                row_dict["recursive_subclass_of"] = ujson.dumps(
                    list(recursive_subclass_concepts)
                )

                intermediate_writer.writerow(row_dict)

        with open(tmp_intermediate_path, "r") as fr, open(output_path, "w") as fw:
            reader = csv.DictReader(fr, **csv_format_params)
            writer = csv.DictWriter(fw, **csv_format_params)
            writer.writeheader()
            for row in itertools.islice(reader, 1, None):
                inlinks = ujson.loads(row[f"{wiki_name}_inlinks"])
                outlinks = ujson.loads(row[f"{wiki_name}_outlinks"])

                inlinks = [
                    (name_to_id[k], cnt) for k, cnt in inlinks if k in name_to_id
                ]
                outlinks = [
                    (name_to_id[k], cnt) for k, cnt in outlinks if k in name_to_id
                ]

                row[f"{wiki_name}_inlinks"] = ujson.dumps(inlinks)
                row[f"{wiki_name}_outlinks"] = ujson.dumps(outlinks)
                writer.writerow(row)
    
        print(f"""
STATS:
Considered: {num_considered}
Empty: {(100 * num_empty / num_considered):.2f}%
{wiki_name} Aliases: {100 * num_aliases / num_considered:.2f}%
{wiki_name} Missing Title: {100 * num_no_title / num_considered:.2f}%
{wiki_name} Missing Article: {100 * num_missing_article / num_considered:.2f}%
    """.strip())


def _row_dict_from_line(
    line, wiki_to_article_shelf, wiki_to_alias_shelf, parent_finder, whitelisted_wikis=None
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
            article = shelf.get(title)
            if not article:
                alias = wiki_to_alias_shelf[wiki].get(title)
                if alias:
                    article = shelf.get(alias)

            if article:
                row_dict[f"{wiki}_title"] = article.title
                row_dict[f"{wiki}_pagerank"] = article.pagerank
            else:
                row_dict[f"{wiki}_title"] = title
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


def write_csv(
    wikidata_path,
    output_path,
    wiki_to_article_shelf,
    wiki_to_alias_shelf,
    parent_finder,
    whitelisted_wikis=None,
    limit=None,
    concurrency=None,
):
    wikis = set(wiki_to_article_shelf.keys())
    if whitelisted_wikis:
        wikis &= set(whitelisted_wikis)

    if set(wiki_to_alias_shelf.keys()) != wikis:
        raise RuntimeError("Missing or additional alias shelf")

    logger.info(f"Writing CSVs for {wikis}")

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
                + [
                    "direct_instance_of",
                    "recursive_instance_of",
                    "direct_subclass_of",
                    "recursive_subclass_of",
                ]
            ),
        )

        writer.writeheader()

        for line in itertools.islice(buffered_lines_with_progress(wikidata_path), limit):
            row_dict = _row_dict_from_line(
                line, wiki_to_article_shelf, wiki_to_alias_shelf, parent_finder, whitelisted_wikis=whitelisted_wikis,
            )

            if not row_dict:
                continue

            writer.writerow(row_dict)


def wikidata_inheritance_graph(input_path, limit=None):
    stream = buffered_lines_with_progress(input_path)
    return WikiDataParser.inheritance_graph(stream, limit=limit)