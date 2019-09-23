import ujson as json
import time
import pickle
from itertools import zip_longest
from collections import namedtuple
from dataclasses import dataclass
from typing import Optional


GlobeCoordinate = namedtuple("GlobeCoordinate", ["latitude", "longitude", "altitude", "precision"])


@dataclass
class WikiDataEntry:
    id: str
    sample_name: str
    sample_coord: Optional[GlobeCoordinate]
    instance_of_city: bool


class WikiData:
    def __init__(self, wiki_title_to_id, id_to_entry):
        self.wiki_title_to_id = wiki_title_to_id
        self.id_to_entry = id_to_entry

    def wikidata_id(self, wikiname, title):
        return self.wiki_title_to_id[wikiname][title]

    @classmethod
    def load(cls, path):
        return pickle.load(open(path, "rb"))

    def dump(self, path):
        pickle.dump(self, open(path, "wb"), protocol=pickle.HIGHEST_PROTOCOL)


def grouper(n, iterable, padvalue=None):
    "grouper(3, 'abcdefg', 'x') --> ('a','b','c'), ('d','e','f'), ('g','x','x')"
    return zip_longest(*[iter(iterable)] * n, fillvalue=padvalue)


class WikiDataParser:

    @classmethod
    def extract_snak_value(cls, item, expected_value_type, title="", line_id=""):
        if "mainsnak" not in item:
            raise RuntimeError(f"({line_id} {title}) Main snak not found in item {item}")

        mainsnak = item["mainsnak"]
        if "snaktype" not in mainsnak:
            raise RuntimeError(f"({line_id} {title}) Main snak missing snaktype")

        if mainsnak["snaktype"] != "value":
            return None

        if "datavalue" not in mainsnak:
            raise RuntimeError(f"({line_id} {title}) Main snak of value type without data value")

        datavalue = mainsnak["datavalue"]
        if "type" not in datavalue or datavalue["type"] != expected_value_type:
            raise RuntimeError(f"({line_id} {title}) Expected type {expected_value_type} in {datavalue}")

        return datavalue["value"]

    @classmethod
    def parse_instance_of_city(cls, claims, title="", line_id=""):
        if "P31" not in claims:
            return False

        instances = claims["P31"]
        for instance in instances:
            value = cls.extract_snak_value(instance, "wikibase-entityid", title=title, line_id=line_id)
            if (
                value is not None
                and "entity-type" in value
                and value["entity-type"] == "item"
                and "id" in value
                and value["id"] == "Q515"
            ):
                return True

        return False

    @classmethod
    def parse_globe_coordinate(cls, claims, title="", line_id=""):
        if "P625" not in claims:
            return None

        coordinates = claims["P625"]
        value = cls.extract_snak_value(coordinates[0], "globecoordinate", title=title, line_id=line_id)
        if value is None:
            return None

        return GlobeCoordinate(*(value[e] for e in ("latitude", "longitude", "altitude", "precision")))

    @classmethod
    def parse_dump(cls, input_stream, whitelisted_wikis=None, limit=None):
        wiki_title_to_id = {}
        id_to_entry = {}
        start = time.time()
        all_json_time = 0

        whitelisted_wikis = whitelisted_wikis and set(whitelisted_wikis)

        for i, line in enumerate(input_stream):
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

            english_title = None
            sample_title = None

            for wiki, v in loaded["sitelinks"].items():
                if whitelisted_wikis is not None and wiki not in whitelisted_wikis:
                    continue

                title = v["title"]
                if wiki not in wiki_title_to_id:
                    wiki_title_to_id[wiki] = {}

                if wiki == "enwiki":
                    english_title = title

                sample_title = title

                wiki_title_to_id[wiki][title] = line_id

            sample_title = english_title or sample_title
            claims = loaded["claims"]
            sample_coord = cls.parse_globe_coordinate(claims, sample_title, line_id)
            instance_of_city = cls.parse_instance_of_city(claims)

            entry = WikiDataEntry(
                line_id,
                sample_title,
                sample_coord,
                instance_of_city=instance_of_city
            )
            id_to_entry[line_id] = entry

            if i % 10000 == 0:
                end = time.time()
                print(
                    f"Reached {i} in {end - start}s [{100 * all_json_time / (end - start)}% in json]"
                    f"({i / (end - start)} lines per second)"
                )

            if limit and i >= limit:
                break

        return WikiData(wiki_title_to_id, id_to_entry)
