# Commands to run in order to create a subset of the original database.

ATTACH DATABASE 'yandex.db' AS 'yandex';

CREATE TABLE users AS
	SELECT DISTINCT user 
	FROM 	yandex.session
				WHERE day > 25
				GROUP BY user
				ORDER BY RANDOM()
				LIMIT 100000;

CREATE TABLE session AS
  SELECT session_id, user, day
  FROM  yandex.session
        NATURAL JOIN users;

CREATE INDEX sessionIndex on session(session_id, user);

CREATE TABLE serp AS
  SELECT serp_id, session_id, serp, time_passed, query_id, is_test
  FROM  yandex.serp
        NATURAL JOIN session;

CREATE INDEX serpIndex on serp(serp_id, session_id);

CREATE TABLE query AS
	SELECT query_id, query
	FROM  yandex.query
				NATURAL JOIN serp;

CREATE INDEX queryIndex on query(query_id);

CREATE TABLE serpitem AS
	SELECT serp_id, position, site
	FROM  yandex.serpitem
				NATURAL JOIN serp;

CREATE INDEX serpitemIndex1 on serpitem(serp_id);
CREATE INDEX serpitemIndex3 on serpitem(site);

CREATE TABLE relevance AS
	SELECT serp_id, site, dwell_time
	FROM  yandex.relevance
				NATURAL JOIN serp;

CREATE INDEX relevanceIndex1 on relevance(serp_id);
CREATE INDEX relevanceIndex2 on relevance(site);

CREATE TABLE clicks AS
	SELECT serp_id, time_passed, site
	FROM yandex.clicks
				NATURAL JOIN serp;

CREATE INDEX clicksIndex1 on relevance(serp_id);
CREATE INDEX clicksIndex2 on relevance(site);

CREATE TABLE sites AS
	SELECT distinct site, domain
	FROM  yandex.sites
				NATURAL JOIN serpitem;

CREATE INDEX sitesIndex1 on sites(site);
CREATE INDEX sitesIndex2 on sites(domain);