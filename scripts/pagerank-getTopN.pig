/*
 * arguments:
 *   from: read previouse iteration result from pagerank-working/iteration-$from
 *   n   : for top N query
 */
nPage = LOAD 'pagerank-working/iteration-$from' AS (id: int,currentRank: double,links: {(link: int)});

-- top n
pageWithIdRank = FOREACH nPage GENERATE id, currentRank;
orderedPage = ORDER pageWithIdRank BY currentRank DESC;-- PARALLEL 5;
topNPageId = LIMIT orderedPage (int) '$n';

-- covert id to title
lut = LOAD 'pagerank-working/lookup-table' AS (id: int, title: chararray);
topNWithRedundantId = JOIN topNPageId BY id, lut BY id USING 'replicated';
topNPage = FOREACH topNWithRedundantId GENERATE title, currentRank;

STORE topNPage INTO 'pagerank-working/top-$n-result';
