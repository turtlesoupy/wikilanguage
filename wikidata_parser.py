from bidict import bidict
import pygtrie
import ujson as json
import time
import pickle
from itertools import zip_longest
import datetime
from collections import namedtuple, defaultdict
import graph_tool
import graph_tool.search
from graph_tool import GraphView
from typing import Iterator

GlobeCoordinate = namedtuple("GlobeCoordinate", ["latitude", "longitude", "altitude", "precision"])
WikiDataEntry = namedtuple(
    "WikiDataEntry",
    [
        "id",
        "sample_label",
        "sample_coord",
        "publication_date",
        "country_of_origin",
        "titles_by_wiki",
        "direct_instance_of",
    ],
)


class WikiDataProperties:
    SUBCLASS_OF = "P279"
    INSTANCE_OF = "P31"
    COORDINATE_LOCATION = "P625"
    COUNTRY_OF_ORIGIN = "P495"
    PUBLICATION_DATE = "P577"


class ParentFinder:
    def __init__(self, parents):
        self.parents = parents

    def all_parents(self, the_id, add_to_set=None):
        the_set = set() if add_to_set is None else add_to_set

        def _inner(the_id):
            if the_id in the_set:
                return

            the_set.add(the_id)
            for p in self.parents[the_id]:
                _inner(p)

        _inner(the_id)
        return the_set


class WikiDataInheritanceGraph:
    def __init__(self, line_id_to_idx, line_id_to_label, graph):
        self.line_id_to_idx = line_id_to_idx
        self.line_id_to_label = line_id_to_label
        self.graph = graph

    def parent_finder(self):
        # Graph tool is a bit slow for DFS on this large graph, not sure why

        parents = defaultdict(set)
        for e in self.graph.edges():
            f, t = self.id_for_edge(e)
            parents[t].add(f)

        return ParentFinder(parents)

    def has_id(self, the_id):
        return the_id in self.line_id_to_idx

    def idx_for_vertex(self, vertex):
        return self.graph.vertex_index[vertex]

    def idx_for_edge(self, edge):
        return (self.idx_for_vertex(edge.source()), self.idx_for_vertex(edge.target()))

    def idx_for_id(self, the_id):
        return self.line_id_to_idx[the_id]

    def id_for_idx(self, idx):
        return self.line_id_to_idx.inverse[idx]

    def id_for_vertex(self, vertex):
        return self.id_for_idx(self.idx_for_vertex(vertex))

    def id_for_edge(self, edge):
        return (self.id_for_vertex(edge.source()), self.id_for_vertex(edge.target()))

    def label_for_vertex(self, vertex):
        return self.label_for_idx(self.idx_for_vertex(vertex))

    def label_for_edge(self, edge):
        id1, id2 = self.idx_for_edge(edge)
        return (self.label_for_idx(id1), self.label_for_idx(id2))

    def label_for_id(self, the_id):
        return self.line_id_to_label[the_id]

    def label_for_idx(self, idx):
        return self.label_for_id(self.id_for_idx(idx))

    def vertex_for_id(self, the_id):
        return self.graph.vertex(self.idx_for_id(the_id))

    def descendent_ids(self, vertex_id):
        seen_set = set()
        for edge in graph_tool.search.dfs_iterator(self.graph, self.idx_for_id(vertex_id)):
            if edge.target() in seen_set:
                continue

            seen_set.add(edge.target())
            yield self.id_for_vertex(edge.target())

    @classmethod
    def load(cls, path):
        with open(path, "rb") as f:
            return pickle.load(f)

    def dump(self, path):
        with open(path, "wb") as f:
            pickle.dump(self, f, protocol=pickle.HIGHEST_PROTOCOL)


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
    def parse_instances(cls, claims, instance_map, title="", line_id=""):
        if WikiDataProperties.INSTANCE_OF not in claims:
            return set()

        ret = set()
        instances = claims[WikiDataProperties.INSTANCE_OF]
        for instance in instances:
            value = cls.extract_snak_value(instance, "wikibase-entityid", title=title, line_id=line_id)
            if value is not None and "entity-type" in value and value["entity-type"] == "item" and "id" in value:
                ret.add(value["id"])

        return ret

    @classmethod
    def parse_country_of_origin(cls, claims, title="", line_id=""):
        if WikiDataProperties.COUNTRY_OF_ORIGIN not in claims:
            return None

        instances = claims[WikiDataProperties.COUNTRY_OF_ORIGIN]
        if not instances:
            return None

        value = cls.extract_snak_value(instances[0], "wikibase-entityid", title=title, line_id=line_id)

        if value is not None and "entity-type" in value and value["entity-type"] == "item" and "id" in value:
            return value["id"]

        return None

    @classmethod
    def parse_globe_coordinate(cls, claims, title="", line_id=""):
        if WikiDataProperties.COORDINATE_LOCATION not in claims:
            return None

        coordinates = claims[WikiDataProperties.COORDINATE_LOCATION]
        value = cls.extract_snak_value(coordinates[0], "globecoordinate", title=title, line_id=line_id)
        if value is None:
            return None

        return GlobeCoordinate(*(value[e] for e in ("latitude", "longitude", "altitude", "precision")))

    @classmethod
    def parse_time(cls, value, title="", line_id=""):
        if "time" not in value:
            print(f"WARN: ({line_id} {title}) missing time key in date {value}")
            return None
        time = value["time"]

        if "precision" not in value:
            print(f"WARN: ({line_id} {title}) missing precision key in date {value}")
            return None
        precision = value["precision"]

        if "calendarmodel" not in value:
            print(f"WARN: ({line_id} {title}) missing calendar model in date {value}")
            return None
        cal = value["calendarmodel"]

        if cal != "http://www.wikidata.org/entity/Q1985727":
            return None

        if time[0] != "+":
            return None

        try:
            if precision == 7:
                parsed = datetime.datetime.strptime(value["time"][:5], "+%Y")
            elif precision == 8:
                parsed = datetime.datetime.strptime(value["time"][:5], "+%Y")
            elif precision == 9:
                parsed = datetime.datetime.strptime(value["time"][:5], "+%Y")
            elif precision == 10:
                parsed = datetime.datetime.strptime(value["time"][:8], "+%Y-%m")
            elif precision == 11:
                parsed = datetime.datetime.strptime(value["time"][:11], "+%Y-%m-%d")
            elif precision == 12:
                parsed = datetime.datetime.strptime(value["time"][:14], "+%Y-%m-%dT%H")
            elif precision == 12:
                parsed = datetime.datetime.strptime(value["time"][:17], "+%Y-%m-%dT%H:%M")
            elif precision == 14:
                parsed = datetime.datetime.strptime(value["time"], "+%Y-%m-%dT%H:%M:%SZ")
            else:
                return None
        except ValueError:
            print(f"WARN: ({line_id} {title}) missing value error during parsing {value}")
            return None

        return parsed.replace(tzinfo=datetime.timezone.utc)

    @classmethod
    def parse_min_publication_date(cls, claims, title="", line_id=""):
        if WikiDataProperties.PUBLICATION_DATE not in claims:
            return None

        min_date = None
        dates = claims[WikiDataProperties.PUBLICATION_DATE]
        for date in dates:
            value = cls.extract_snak_value(date, "time", title=title, line_id=line_id)
            if value is None:
                print(f"WARN: Unexpected date {date}")
                continue

            time = cls.parse_time(value)
            if time and (not min_date or time < min_date):
                min_date = time

        return int(min_date.timestamp()) if min_date else None

    @classmethod
    def inheritance_graph(cls, input_stream, limit=None):
        g = graph_tool.Graph()
        line_id_to_idx = bidict()
        line_id_to_label = {}

        def edge_yielder():
            current_property_idx = 0
            num_edges = 0
            start = time.time()

            for i, line in enumerate(input_stream):
                if limit and i >= limit:
                    break

                if i % 10000 == 0:
                    delta = time.time() - start
                    print(
                        f"Reached line {i} with {len(line_id_to_idx)} properties and {num_edges} edges"
                        f" @ {delta}s ({i / delta} LPS)"
                    )

                if not line.startswith("{"):
                    continue

                loaded = json.loads(line.rstrip(",\n"))

                line_type = loaded["type"]
                line_id = loaded["id"]

                if line_type == "property":
                    continue

                if line_type != "item":
                    raise RuntimeError("Found non-item line")

                if line_id not in line_id_to_idx:
                    line_id_to_idx[line_id] = current_property_idx
                    current_property_idx += 1

                label = "<UNKNOWN>"
                if "labels" in loaded and "en" in loaded["labels"]:
                    label = loaded["labels"]["en"]["value"]

                line_id_to_label[line_id] = label

                claims = loaded["claims"]

                if WikiDataProperties.SUBCLASS_OF not in claims:
                    continue

                superclasses = claims[WikiDataProperties.SUBCLASS_OF]
                for superclass in superclasses:
                    value = cls.extract_snak_value(superclass, "wikibase-entityid", line_id=line_id)
                    if (
                        value is not None
                        and "entity-type" in value
                        and value["entity-type"] == "item"
                        and "id" in value
                    ):
                        superclass_id = value["id"]
                        if superclass_id not in line_id_to_idx:
                            line_id_to_idx[superclass_id] = current_property_idx
                            current_property_idx += 1

                        yield (line_id_to_idx[superclass_id], line_id_to_idx[line_id])
                        num_edges += 1

        g.add_edge_list(edge_yielder())
        return WikiDataInheritanceGraph(line_id_to_idx, line_id_to_label, g)

    @classmethod
    def parse_dump_line(cls, line, whitelisted_wikis=None):
        if not line.startswith("{"):
            return None

        loaded = json.loads(line.rstrip(",\n"))

        line_type = loaded["type"]
        line_id = loaded["id"]

        if line_type == "property":
            return None

        if line_type != "item":
            print(json.dumps(loaded, indent=2))
            print(line_type)
            raise RuntimeError("Found non-item line")

        if "sitelinks" not in loaded:
            print(loaded)
            raise RuntimeError("No sitelinks found in entry")

        if "labels" not in loaded:
            print(loaded)
            raise RuntimeError("No labels in entry")

        try:
            sample_label = loaded["labels"].get("en", next(iter(loaded["labels"].values())))["value"]
        except StopIteration:
            sample_label = None

        titles_by_wiki = {}

        for wiki, v in loaded["sitelinks"].items():
            if whitelisted_wikis is not None and wiki not in whitelisted_wikis:
                continue

            title = v["title"]
            titles_by_wiki[wiki] = title

        if not titles_by_wiki:
            return None

        claims = loaded["claims"]

        return WikiDataEntry(
            line_id,
            sample_label,
            sample_coord=cls.parse_globe_coordinate(claims, sample_label, line_id),
            titles_by_wiki=titles_by_wiki,
            direct_instance_of=cls.parse_instances(claims, sample_label, line_id),
            country_of_origin=cls.parse_country_of_origin(claims, sample_label, line_id),
            publication_date=cls.parse_min_publication_date(claims, sample_label, line_id),
        )
