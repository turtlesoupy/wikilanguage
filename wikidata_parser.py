import ujson as json
import time
import pickle
from itertools import zip_longest
from collections import namedtuple


GlobeCoordinate = namedtuple("GlobeCoordinate", ["latitude", "longitude", "altitude", "precision"])
WikiDataEntry = namedtuple("WikiDataEntry", ["id", "sample_name", "sitelinks", "sample_coord"])


def grouper(n, iterable, padvalue=None):
    "grouper(3, 'abcdefg', 'x') --> ('a','b','c'), ('d','e','f'), ('g','x','x')"
    return zip_longest(*[iter(iterable)] * n, fillvalue=padvalue)


class WikiDataParser:
    @classmethod
    def load(cls, path):
        return pickle.load(open(path, "rb"))

    def dump(self, path):
        pickle.dump(self, open(path, "wb"), protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    def parse_globe_coordinate(cls, claims, title="", line_id=""):
        if "P625" not in claims:
            return None

        coordinates = claims["P625"]
        # if len(coordinates) != 1:
        #    print(json.dumps(coordinates, indent=2))
        #    raise RuntimeError(f"({title}) Found item with more than one co-ordinate")

        coordinate = coordinates[0]
        if "mainsnak" not in coordinate:
            print(coordinate)
            raise RuntimeError(f"({line_id} {title}) Main snak not found in co-ordinate")

        mainsnak = coordinate["mainsnak"]
        if "snaktype" not in mainsnak:
            print(mainsnak)
            raise RuntimeError(f"({line_id} {title}) Main snak snaktype")

        if mainsnak["snaktype"] != "value":
            return None

        if "datavalue" not in mainsnak:
            print(mainsnak)
            raise RuntimeError(f"({line_id} {title}) Main snak without data value")

        datavalue = mainsnak["datavalue"]
        if "type" not in datavalue or datavalue["type"] != "globecoordinate":
            print(datavalue)
            raise RuntimeError(f"({line_id} {title}) Bad data type for globe coordinate")

        value = datavalue["value"]
        return GlobeCoordinate(*(value[e] for e in ("latitude", "longitude", "altitude", "precision")))

    @classmethod
    def parse_dump(cls, path, whitelisted_wikis=None):
        wiki_title_to_id = {}
        id_to_entry = {}
        start = time.time()
        all_json_time = 0

        whitelisted_wikis = whitelisted_wikis and set(whitelisted_wikis)

        for i, line in enumerate(open(path)):
            if not line.startswith("{"):
                continue

            s_json = time.time()
            loaded = json.loads(line.rstrip(",\n"))
            all_json_time += time.time() - s_json

            line_type = loaded["type"]
            line_id = loaded["id"]

            if line_type == "property":
                continue

            if line_type != "item":
                print(json.dumps(loaded, indent=2))
                print(line_type)
                raise RuntimeError("Found non-item line")

            if "sitelinks" not in loaded:
                print(loaded)
                raise RuntimeError("No sitelinks found in entry")

            sitelinks = {}

            for wiki, v in loaded["sitelinks"].items():
                if whitelisted_wikis is not None and wiki not in whitelisted_wikis:
                    continue

                title = v["title"]
                if wiki not in wiki_title_to_id:
                    wiki_title_to_id[wiki] = {}

                sitelinks[wiki] = title
                wiki_title_to_id[wiki][title] = line_id

            try:
                sample_title = sitelinks["enwiki"] if "enwiki" in sitelinks else next(iter(sitelinks.values()))
            except StopIteration:
                sample_title = None

            sample_coord = cls.parse_globe_coordinate(loaded["claims"], sample_title, line_id)

            entry = WikiDataEntry(line_id, sample_title, sitelinks, sample_coord)
            id_to_entry[line_id] = entry

            if i % 10000 == 0:
                end = time.time()
                print(
                    f"Reached {i} in {end - start}s [{100 * all_json_time / (end - start)}% in json]"
                    f"({i / (end - start)} lines per second)"
                )

        return cls(wiki_title_to_id, id_to_entry)

    def __init__(self, wiki_title_to_id, id_to_entry):
        self.wiki_title_to_id = wiki_title_to_id
        self.id_to_entry = id_to_entry

    def wikidata_id(self, wikiname, title):
        return self.wiki_title_to_id[wikiname][title]
