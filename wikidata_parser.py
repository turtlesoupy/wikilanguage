from bidict import bidict
import pygtrie
import ujson as json
import time
import pickle
from itertools import zip_longest
from collections import namedtuple
import graph_tool
import graph_tool.search


GlobeCoordinate = namedtuple(
    "GlobeCoordinate", ["latitude", "longitude", "altitude", "precision"]
)
WikiDataEntry = namedtuple("WikiDataEntry", ["id", "sample_coord", "instances"])


class WikiDataProperties:
    SUBCLASS_OF = "P279"
    INSTANCE_OF = "P31"
    COORDINATE_LOCATION = "P625"


class WikiData:
    def __init__(self, wiki_title_to_id, id_to_entry):
        self.wiki_title_to_id = wiki_title_to_id
        self.id_to_entry = id_to_entry

    def wikidata_id(self, wikiname, title):
        return self.wiki_title_to_id[wikiname][title]

    def restrict_to_wikis(self, valid_wikis):
        for wiki in list(self.wiki_title_to_id.keys()):
            if wiki not in valid_wikis:
                del self.wiki_title_to_id[wiki]

    @classmethod
    def load(cls, path):
        with open(path, "rb") as f:
            return pickle.load(f)

    def dump(self, path):
        with open(path, "wb") as f:
            pickle.dump(self, f, protocol=pickle.HIGHEST_PROTOCOL)


class WikiDataInheritanceGraph:
    def __init__(self, line_id_to_idx, line_id_to_label, graph):
        self.line_id_to_idx = line_id_to_idx
        self.line_id_to_label = line_id_to_label
        self.graph = graph

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
        for edge in graph_tool.search.bfs_iterator(
            self.graph, self.idx_for_id(vertex_id)
        ):
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
            raise RuntimeError(
                f"({line_id} {title}) Main snak not found in item {item}"
            )

        mainsnak = item["mainsnak"]
        if "snaktype" not in mainsnak:
            raise RuntimeError(f"({line_id} {title}) Main snak missing snaktype")

        if mainsnak["snaktype"] != "value":
            return None

        if "datavalue" not in mainsnak:
            raise RuntimeError(
                f"({line_id} {title}) Main snak of value type without data value"
            )

        datavalue = mainsnak["datavalue"]
        if "type" not in datavalue or datavalue["type"] != expected_value_type:
            raise RuntimeError(
                f"({line_id} {title}) Expected type {expected_value_type} in {datavalue}"
            )

        return datavalue["value"]

    @classmethod
    def parse_instances(cls, claims, instance_map, title="", line_id=""):
        if WikiDataProperties.INSTANCE_OF not in claims:
            return False

        instances = claims[WikiDataProperties.INSTANCE_OF]
        for instance in instances:
            value = cls.extract_snak_value(
                instance, "wikibase-entityid", title=title, line_id=line_id
            )
            if (
                value is not None
                and "entity-type" in value
                and value["entity-type"] == "item"
                and "id" in value
            ):
                the_id = value["id"]
                return {
                    instance_name
                    for instance_name, property_set in instance_map.items()
                    if the_id in property_set
                }

        return False

    @classmethod
    def parse_globe_coordinate(cls, claims, title="", line_id=""):
        if WikiDataProperties.COORDINATE_LOCATION not in claims:
            return None

        coordinates = claims[WikiDataProperties.COORDINATE_LOCATION]
        value = cls.extract_snak_value(
            coordinates[0], "globecoordinate", title=title, line_id=line_id
        )
        if value is None:
            return None

        return GlobeCoordinate(
            *(value[e] for e in ("latitude", "longitude", "altitude", "precision"))
        )

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
                    value = cls.extract_snak_value(
                        superclass, "wikibase-entityid", line_id=line_id
                    )
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
    def parse_dump(
        cls, input_stream, whitelisted_wikis=None, instance_map={}, limit=None
    ):
        wiki_title_to_id = {}
        id_to_entry = pygtrie.StringTrie()
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
                    wiki_title_to_id[wiki] = pygtrie.StringTrie()

                if wiki == "enwiki":
                    english_title = title

                sample_title = title

                wiki_title_to_id[wiki][title] = line_id

            sample_title = english_title or sample_title
            if sample_title is None:
                continue

            claims = loaded["claims"]
            sample_coord = cls.parse_globe_coordinate(claims, sample_title, line_id)
            instances = cls.parse_instances(claims, instance_map, sample_title, line_id)

            entry = WikiDataEntry(line_id, sample_coord, instances=instances)
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
