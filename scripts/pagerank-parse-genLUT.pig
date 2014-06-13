/*
 * arguments:
 *   inputPath: wikipedia page input path
 */
DEFINE LinkParser kr.ac.kaist.adward.pagerankpig.udf.PigLinkExtractor();

-- extract links
pageInText = LOAD '$inputPath' USING TextLoader AS (line: chararray);
pages = FOREACH pageInText GENERATE FLATTEN(LinkParser(line)) AS (id: int, title: chararray, links: {(link: chararray)});

-- validPages: {id: int,title: chararray,links: {(link: chararray)}}
validPages = FILTER pages BY (title IS NOT NULL);

-- save text-based link graph
STORE validPages INTO 'pagerank-working/links';
validPages = LOAD 'pagerank-working/links' AS (id: int, title: chararray, links: {(link: chararray)});

-- generate LUT
lut = FOREACH validPages GENERATE id AS id, title AS title;
STORE lut INTO 'pagerank-working/lookup-table';
