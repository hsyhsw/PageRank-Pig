/*
 * arguments:
 *   from: read previouse iteration result from pagerank-working/iteration-$from
 *   to  : write $from + 1 iteration result to pagerank-working/iteration-$to
 */
nPage = LOAD 'pagerank-working/iteration-$from' AS (id: int,currentRank: double,links: {(link: int)});

-- one iteration
-- originalOutgoingLinks: {(id: int,links: {(link: int)})}
originalOutgoingLinks = FOREACH nPage GENERATE id AS id, links AS links;
-- outgoingLinksWithDelta: {links::link: int,delta: double}
outgoingLinksWithDelta = FOREACH nPage GENERATE FLATTEN(links), currentRank / COUNT(links) AS delta;
-- groupedOutgoingLinks: {group: int,outgoingLinksWithDelta: {(links::link: int,delta: double)},originalOutgoingLinks: {(id: int,links: {(link: int)})}}
groupedOutgoingLinks = COGROUP outgoingLinksWithDelta BY link, originalOutgoingLinks BY id;

-- nPage: {id: int,newRank: double,links: {(link: int)}}
nPage = FOREACH groupedOutgoingLinks GENERATE
	group AS id,
	(IsEmpty(outgoingLinksWithDelta) ? 0.15 : 0.15 + 0.85 * SUM(outgoingLinksWithDelta.delta)) AS newRank,
	FLATTEN(originalOutgoingLinks.links) AS links;

STORE nPage INTO 'pagerank-working/iteration-$to';
