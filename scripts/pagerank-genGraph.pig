/*
 * arguments:
 *   N/A
 */
DEFINE IdLUT kr.ac.kaist.adward.pagerankpig.udf.IdLUT('pagerank-working/lookup-table');

validPages = LOAD 'pagerank-working/links' AS (id: int, title: chararray, links: {(link: chararray)});

-- convert text-based link graph to id-based link graph
--allLink = FOREACH validPages GENERATE id AS referrerId, FLATTEN(links);
--linksWithRedundantTitle = JOIN allLink BY link, lut BY title USING 'replicated';
--linksWithId = FOREACH linksWithRedundantTitle GENERATE allLink::referrerId, lut::id;
--groupedLink = COGROUP linksWithId BY allLink::referrerId, lut BY id;
--nPage = FOREACH groupedLink GENERATE group AS referrerId, 1.0 AS currentRank, linksWithId.id AS links;
nPage = FOREACH validPages {
	linkId = FOREACH links GENERATE IdLUT(link) AS link;
	linkIdNotNull = FILTER linkId BY link is not null;
	GENERATE id AS referrer, 1.0 AS currentRank, linkIdNotNull AS links;
};

STORE nPage INTO 'pagerank-working/iteration-00';
