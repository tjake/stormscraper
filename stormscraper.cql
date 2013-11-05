CREATE KEYSPACE stormscraper WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE stormscraper;

CREATE TABLE scrape_list (
	url text PRIMARY KEY,
	last_update timestamp,
	depth int
);

CREATE TABLE pages ( 
	url text,
	scrape_date timestamp,
	title text,
	html text,
	text text,
	inbound_links set<text>,
	outbound_links set<text>,
	PRIMARY KEY (url, scrape_date)
);

INSERT INTO scrape_list(url,last_update,depth)VALUES('http://storm-project.net','0001-01-01', 1);
INSERT INTO scrape_list(url,last_update,depth)VALUES('http://cassandra.apache.org','0001-01-01', 0);