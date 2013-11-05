package com.github.tjake.stormscraper.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.datastax.driver.core.*;
import com.github.tjake.stormscraper.bolt.CassandraWriterBolt;
import com.github.tjake.stormscraper.bolt.ScraperBolt;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class ScraperSpout extends BaseRichSpout {

    static final Logger logger = LoggerFactory.getLogger(ScraperSpout.class);

    Map<String,String> config;
    SpoutOutputCollector collector;
    Cluster cassandraCluster;
    Session session;

    //Tracks what's being worked on
    Cache<Object, Date> workCache;

    //List of urls to scrape
    Iterator<Row> tableIterator = null;
    int updateDelayMinutes;
    int maxScrapeDepth;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {

        this.collector = collector;
        config = (Map<String,String>) map;

        updateDelayMinutes = Integer.valueOf(config.get("update.delay.minutes"));
        maxScrapeDepth = Integer.valueOf(config.get("max.scrape.depth"));

        Preconditions.checkArgument(updateDelayMinutes > 0, "update.delay.minutes must be set positive");
        Preconditions.checkArgument(maxScrapeDepth > 0, "max.scrape.depth must be set positive");


        workCache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();
        cassandraCluster = CassandraWriterBolt.setupCassandraClient(config.get("cassandra.nodes").split(","));
        session = CassandraWriterBolt.getSessionWithRetry(cassandraCluster,config.get("cassandra.keyspace"));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(ScraperBolt.boltFields);
    }


    private void maybeCreateIterator() {
        if (tableIterator != null && tableIterator.hasNext())
            return;

        //We have exausted the iterator and need to re-init. Sleep for a few.
        if (tableIterator != null) {
            logger.debug("Exhausted scrape list, pausing before next check");
            Utils.sleep(1000);
        }



        ResultSet result = session.execute("select url, last_update, depth from scrape_list");

        //FIXME: This doesn't scale, iterator should be lazy
        tableIterator = result.all().iterator();

    }

    @Override
    public void nextTuple() {

        maybeCreateIterator();

        //We may have an empty table
        if (!tableIterator.hasNext())
            return;

        Row row = tableIterator.next();

        String url = row.getString("url");
        Integer depth = row.getInt("depth");

        //Enforce max depth setting
        if (depth < 0 || depth > maxScrapeDepth) {
            logger.warn("Setting scrape depth from {} to configured max {}", depth, maxScrapeDepth);
            depth = maxScrapeDepth;
        }

        Date lastUpdate = row.getDate("last_update");

        //See if the delay has elapsed
        Calendar cal = Calendar.getInstance();
        cal.setTime(lastUpdate);
        cal.add(Calendar.MINUTE, updateDelayMinutes);

        //Too early
        if (cal.getTime().after(new Date()))
            return;

        //Are we already working on this url?
        if (workCache.getIfPresent(url) != null)
            return;

        //Scrape it!
        logger.info("Emitting url: {} for {} levels deep", url, depth);
        Date now = new Date();
        workCache.put(url, now); //mark the start time
        collector.emit(new Values(url,url,now,depth), url);
    }

    @Override
    public void ack(Object id)
    {
        String url = (String) id;
        Date startTime = workCache.getIfPresent(url);

        if (startTime == null) {
            logger.warn("Scraping took > 10min!: {}", url);
            return;
        }

        //update the cache
        logger.info("Scraping complete for: {}", url);
        PreparedStatement ps = session.prepare("UPDATE scrape_list SET last_update = ? where url = ?");

        BoundStatement bs = ps.bind(startTime, url);
        session.execute(bs);


        workCache.invalidate(url);
    }

    @Override
    public void fail(Object id)
    {
        logger.warn("Scraping failed for: {}", id);
        workCache.invalidate(id);
    }
}
