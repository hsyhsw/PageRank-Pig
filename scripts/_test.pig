REGISTER pagerank-pig.jar;
DEFINE LinkParser kr.ac.kaist.adward.pagerankpig.udf.PigLinkExtractor();
DEFINE IdLUT kr.ac.kaist.adward.pagerankpig.udf.IdLUT('pagerank-working/lookup-table');

-- extract links
pageInText = LOAD 'input' USING TextLoader AS (line: chararray);
pages = FOREACH pageInText GENERATE FLATTEN(LinkParser(line)) AS (id: int, title: chararray, links: {(link: chararray)});

-- validPages: {id: int,title: chararray,links: {(link: chararray)}}
validPages = FILTER pages BY (title IS NOT NULL);

-- save text-based link graph
STORE validPages INTO 'pagerank-working/links';
validPages = LOAD 'pagerank-working/links' AS (id: int, title: chararray, links: {(link: chararray)});

-- generate LUT
lut = FOREACH validPages GENERATE id AS id, title AS title;
STORE lut INTO 'pagerank-working/lookup-table';

-- convert text-based link graph to id-based link graph
-- allLink = FOREACH validPages GENERATE id AS referrerId, FLATTEN(links);
-- linksWithRedundantTitle = JOIN allLink BY link, lut BY title USING 'replicated';
-- linksWithId = FOREACH linksWithRedundantTitle GENERATE allLink::referrerId, lut::id;
-- groupedLink = COGROUP linksWithId BY allLink::referrerId, lut BY id;
-- nPage = FOREACH groupedLink GENERATE group AS referrerId, 1.0 AS currentRank, linksWithId.id AS links;
nPage = FOREACH validPages {
	linkId = FOREACH links GENERATE IdLUT(link) AS link;
	linkIdNotNull = FILTER linkId BY link is not null;
	GENERATE id AS referrer, 1.0 AS currentRank, linkIdNotNull AS links;
};
STORE nPage INTO 'pagerank-working/iteration-00';

-- one iteration
nPage = LOAD 'pagerank-working/iteration-00' AS (id: int, currentRank: double, links: {(link: int)});
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

STORE nPage INTO 'pagerank-working/iteration-01';

nPage = LOAD 'pagerank-working/iteration-01' AS (id: int,currentRank: double,links: {(link: int)});

-- top n
pageWithIdRank = FOREACH nPage GENERATE id, currentRank;
orderedPage = ORDER pageWithIdRank BY currentRank DESC;-- PARALLEL 5;
topNPageId = LIMIT orderedPage 200;

-- covert id to title
lut = LOAD 'pagerank-working/lookup-table' AS (id: int, title: chararray);
topNWithRedundantId = JOIN topNPageId BY id, lut BY id USING 'replicated';
topNPage = FOREACH topNWithRedundantId GENERATE title, currentRank;

STORE topNPage INTO 'pagerank-working/top-200-result';
