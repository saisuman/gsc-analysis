--
-- Queries for creating all tables and permissions. 
--
CREATE DATABASE gscdb;

DROP TABLE gscdb.gscraw;

CREATE TABLE gscdb.gsc_queries (
    query VARCHAR(512) CHARACTER SET utf8,
    querydate DATE,
    article VARCHAR(1024) CHARACTER SET utf8,
    country VARCHAR(5),
    device VARCHAR(10),
    clicks BIGINT,
    impressions BIGINT,
    ctr FLOAT,
    position FLOAT
);


CREATE TABLE gscdb.gsc_counts_exact (
    querydate DATE,
    country VARCHAR(5),
    device VARCHAR(10),
    clicks BIGINT,
    impressions BIGINT,
    ctr FLOAT,
    position FLOAT
);

CREATE USER
'gscuser'@'%' IDENTIFIED BY '__GSCUSER_PASSWORD__';

GRANT ALL PRIVILEGES ON gscdb.gsc_queries TO 'gscuser'@'%';
GRANT ALL PRIVILEGES ON gscdb.gsc_counts_exact TO 'gscuser'@'%';

FLUSH PRIVILEGES;

--
-- Import into per-query tables.
--
LOAD DATA LOCAL INFILE '/mnt/secondary/dev/gsc-analysis/dump/all-withquery.csv' INTO TABLE gscdb.gsc_queries
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'  (
query, querydate, article, country, device, clicks, impressions, ctr, position);

--
-- Import into per-day counts without queries.
--
LOAD DATA LOCAL INFILE '/mnt/secondary/dev/gsc-analysis/dump/all-noquery.csv' INTO TABLE gscdb.gsc_counts_exact
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'  (
querydate, country, device, clicks, impressions, ctr, position);

--
-- Aggregates by country.
--
CREATE TABLE gsc_by_country_exact AS
	SELECT
		country,
        querydate,
        SUM(impressions) as impressions,
        SUM(clicks) as clicks,
        CAST(SUM(clicks) AS FLOAT) / CAST(SUM(impressions) AS FLOAT) AS ctr,
        CAST(SUM(impressions * position) AS FLOAT) / CAST(SUM(impressions) AS FLOAT) AS position
	FROM gsc_counts_exact
    GROUP BY querydate, country;  
