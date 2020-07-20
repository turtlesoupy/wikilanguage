import tempfile
import pipelines
import os
from wikidata_parser import WikiDataInheritanceGraph
import glob
import shelve
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(level=logging.INFO)

    data_dir = Path("/mnt/evo/projects/wikilanguage/data/20200701")
    wikidata_path = data_dir / "wikidata-20200706-all.json.gz"
    wiki_paths = list(data_dir.glob("*-pages-articles*"))
    wiki_paths.sort(key=lambda p: os.stat(p).st_size, reverse=True)

    limit = None
    output_path = data_dir / "wikilanguage.tsv"
    whitelisted_wikis = None
    working_dir = "working-dir-20200701/"

    logging.info(f"Wiki paths: {wiki_paths}")

    # limit = 10000
    # output_path = "data/test.tsv"
    # whitelisted_wikis = {"enwiki", "jawiki"}
    # working_dir = "working-test/"

    if not os.path.exists(wikidata_path):
        raise RuntimeError(f"{wikidata_path} not found!")

    wiki_shelves = {}
    alias_shelves = {}
    temps = []
    try:
        for wiki_path in wiki_paths:
            wikiname = os.path.basename(str(wiki_path)).split("-")[0]
            if whitelisted_wikis is not None and wikiname not in whitelisted_wikis:
                continue

            alias_shelf_working_path = (
                os.path.join(working_dir, f"aliases_{wikiname}") if working_dir else None
            )
            wiki_shelf_working_path = (
                os.path.join(working_dir, f"{wikiname}") if working_dir else None
            )

            # Build main wiki shelf
            if wiki_shelf_working_path and glob.glob(f"{wiki_shelf_working_path}*"):
                logger.info(f"Reading shelf from {wiki_shelf_working_path}")
                shelf = shelve.open(wiki_shelf_working_path)
            else:
                temp = tempfile.NamedTemporaryFile(delete=False)
                temp.close()
                os.remove(temp.name)
                in_memory = wikiname != "enwiki"
                logger.info(
                    f"Main: starting write {wikiname} to {temp.name} (in_memory={in_memory})"
                )
                temps.append(temp)
                shelf = shelve.open(temp.name)
                pipelines.write_articles_to_shelf(
                    shelf, str(wiki_path), rank_in_memory=in_memory, limit=limit
                )
                if wiki_shelf_working_path:
                    for desc in glob.glob(f"{temp.name}*"):
                        _, ext = os.path.splitext(desc)
                        logger.info(
                            f"Copying shelf {desc} to {wiki_shelf_working_path}{ext}"
                        )
                        os.rename(f"{desc}", f"{wiki_shelf_working_path}{ext}")
                    shelf = shelve.open(wiki_shelf_working_path)

            wiki_shelves[wikiname] = shelf

            # Build alias shelves
            if alias_shelf_working_path and glob.glob(f"{alias_shelf_working_path}*"):
                logger.info(f"Reading alias shelf from {alias_shelf_working_path}")
                alias_shelf = shelve.open(alias_shelf_working_path)
            else:
                logger.info(
                    f"Writing alias shelf for {wikiname}"
                )
                temp = tempfile.NamedTemporaryFile(delete=False)
                temp.close()
                os.remove(temp.name)
                temps.append(temp)
                alias_shelf = shelve.open(temp.name)
                pipelines.write_aliases_to_shelf(shelf.items(), alias_shelf)
                
                if alias_shelf_working_path:
                    for desc in glob.glob(f"{temp.name}*"):
                        _, ext = os.path.splitext(desc)
                        logger.info(
                            f"Copying alias shelf {desc} to {alias_shelf_working_path}{ext}"
                        )
                        os.rename(f"{desc}", f"{alias_shelf_working_path}{ext}")
                    alias_shelf = shelve.open(alias_shelf_working_path)

            alias_shelves[wikiname] = alias_shelf

        logger.info(f"Done wiki writes, loading inheritance graph")

        inheritance_working_path = (
            os.path.join(working_dir, f"inheritance.pickle") if working_dir else None
        )
        if inheritance_working_path and os.path.exists(inheritance_working_path):
            logger.info(f"Loading inheritance graph from {inheritance_working_path}")
            inheritance_graph = WikiDataInheritanceGraph.load(inheritance_working_path)
        else:
            inheritance_graph = pipelines.wikidata_inheritance_graph(
                str(wikidata_path), limit=limit
            )
            logger.info(f"Loaded inheritance graph!")
            if inheritance_working_path:
                logger.info(f"Dumping to {inheritance_working_path}")
                inheritance_graph.dump(inheritance_working_path)

        parent_finder = inheritance_graph.parent_finder()
        del inheritance_graph
        logger.info(f"Writing wikidata to {output_path}")
        pipelines.write_csv(
            str(wikidata_path),
            str(output_path),
            wiki_shelves,
            alias_shelves,
            parent_finder,
            limit=limit,
            whitelisted_wikis=whitelisted_wikis,
        )
        logger.info(f"Done write to {output_path}!")

    finally:
        for shelf in wiki_shelves.values():
            shelf.close()

        for temp in temps:
            for desc in glob.glob(f"{temp.name}*"):
                try:
                    os.remove(desc)
                except FileNotFoundError:
                    pass


if __name__ == "__main__":
    main()
