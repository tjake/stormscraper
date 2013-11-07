Storm Scraper
=============

**TL;DR;**  Storm Scraper is an example storm program.  Please do not think it's a production ready.

Storm Scraper is a simple storm topology that let's you crawl a website n-levels deep.
It reads the list of sites to scrape from Cassandra and stores the 
html, incoming links, outgoing links, text.

I've only tested this locally

Setting are in src/main/resources/scraper.properties

To Run:

  * Run Cassandra

  * Create schema
 
````
cqlsh < stormscraper.cql
````

  * Run storm topology locally

````
MAVEN_OPTS=-Xmx1g mvn compile exec:java   
````

Brought to you by [@tjake](http://twitter.com/tjake)