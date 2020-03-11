import numpy as np
import scipy.stats
import graph_tool
import graph_tool.centrality


def pagerank(canonical_collection_fn):
    print("Creating title map!")
    title_to_logical_id = {p.title: i for (i, p) in enumerate(canonical_collection_fn())}

    print("Adding edges")
    g = graph_tool.Graph(directed=True)
    g.add_vertex(n=len(title_to_logical_id))

    edge_weights = g.new_edge_property("double")

    for page in canonical_collection_fn():
        page_logical_id = title_to_logical_id[page.title]
        norm = sum(page.links.values())
        for link, count in page.links.items():
            edge_weights[g.add_edge(page_logical_id, title_to_logical_id[link])] = count / norm

    g.ep["weight"] = edge_weights

    print("Computing pagerank")
    pageranks = graph_tool.centrality.pagerank(g, weight=g.ep.weight)

    print("Done... yielding results")

    for i, page in enumerate(canonical_collection_fn()):
        yield (page, pageranks[i])


def pagerank_with_percentiles(canonical_collection_fn):
    pageranks = np.array(list(pr for (_, pr) in pagerank(canonical_collection_fn)))
    print("Computing percentiles!")
    percentiles = scipy.stats.rankdata(pageranks) / len(pageranks) if len(pageranks) > 0 else [1.0]
    print("Done... yielding pages with percentiles")

    for item in zip(canonical_collection_fn(), pageranks, percentiles):
        yield item
