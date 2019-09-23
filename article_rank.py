import numpy as np
import scipy.special
import scipy.stats
import itertools


class ArticleRank:
    def _find_common_articles(cls, wikidata, from_wikiname, from_articles, to_wikiname, to_articles):
        common_articles = {
            v: [None, None] for v in itertools.chain(
                wikidata.wiki_title_to_id[from_wikiname],
                wikidata.wiki_title_to_id[to_wikiname],
            )
        }

        num_common_article_candidates = len(common_articles)

        print(
            f"Found {num_common_article_candidates} common candidates out of "
            f"{len(wikidata.wiki_title_to_id[from_wikiname])} from and"
            f"{len(wikidata.wiki_title_to_id[to_wikiname])} to"
        )

        not_found_from = 0
        total_from = 0
        for article in from_articles:
            total_from += 1
            try:
                wikidata_id = wikidata.wikidata_id(from_wikiname, article.title)
            except KeyError:
                not_found_from += 1

            if wikidata_id in common_articles:
                common_articles[wikidata_id][0] = article

        not_found_to = 0
        total_to = 0
        for article in to_articles:
            total_to += 1
            try:
                wikidata_id = wikidata.wikidata_id(to_wikiname, article.title)
            except KeyError:
                not_found_to += 1

            if wikidata_id in common_articles:
                common_articles[wikidata_id][1] = article

        print(
            f"{from_wikiname} missing {not_found_from} ({not_found_from / total_from}) in wikidata "
            f"{to_wikiname} missing {not_found_to} ({not_found_to / total_to}) in wikidata"
        )

        found_one_but_not_two = 0
        found_two_but_not_one = 0
        missing_both = 0
        found_both = 0

        del_list = []

        for (wikidata_id, (from_article, to_article)) in common_articles.items():
            if from_article is None and to_article is None:
                missing_both += 1
                del_list.append(wikidata_id)
            elif from_article is None:
                found_one_but_not_two += 1
                del_list.append(wikidata_id)
            elif to_article is None:
                found_two_but_not_one += 1
                del_list.append(wikidata_id)
            else:
                found_both += 1

        print(
            f"Filtered to {found_both} candidates ({found_both / num_common_article_candidates}) with \n"
            f"\t{found_one_but_not_two / num_common_article_candidates} in {from_wikiname} but not {to_wikiname}\n"
            f"\t{found_two_but_not_one / num_common_article_candidates} in {to_wikiname} but not {from_wikiname}\n"
            f"\t{missing_both / num_common_article_candidates} missing in both"
        )

        for wikidata_id in del_list:
            del common_articles[wikidata_id]
        del del_list

        assert len(common_articles) == found_both, "Should only contain valid articles from both wikis"

        return common_articles

    @classmethod
    def kl_divergence_rank(cls, wikidata, from_wikiname, from_articles, to_wikiname, to_articles):
        common_articles = cls._find_common_articles(
            wikidata, from_wikiname, from_articles, to_wikiname, to_articles
        )

        from_prs = np.zeros(len(common_articles))
        to_prs = np.zeros(len(common_articles))

        for i, (from_article, to_article) in enumerate(common_articles.values()):
            from_prs[i] = from_article.pagerank
            to_prs[i] = to_article.pagerank

            assert from_article.pagerank > 0, f"Found zero page-rank in {from_wikiname}'s {from_article.title}"
            assert to_article.pagerank > 0, f"Found zero page-rank in {to_wikiname}'s {to_article.title}"

        # Normalize to coherent probability distribution
        from_prs /= np.sum(from_prs)
        to_prs /= np.sum(to_prs)

        kl = scipy.special.kl_div(from_prs, to_prs)
        ranks = scipy.stats.rankdata(kl) / len(kl)

        return sorted(
            (
                (kl_div, rank, from_article, to_article)
                for (kl_div, rank, (from_article, to_article)) in zip(kl, ranks, common_articles.values())

            ),
            key=lambda x: x[0],
        )
