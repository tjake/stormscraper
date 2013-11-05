package com.github.tjake.stormscraper.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.fetcher.CustomFetchStatus;
import edu.uci.ics.crawler4j.fetcher.PageFetchResult;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.parser.ParseData;
import edu.uci.ics.crawler4j.parser.Parser;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import edu.uci.ics.crawler4j.url.WebURL;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Most of the crawler code comes from crawler4j examples or stack overflow
 */
public class ScraperBolt extends BaseRichBolt{
    private static final Logger logger = LoggerFactory.getLogger(ScraperBolt.class);

    // storm
    Map<String,String> stormConfig;
    OutputCollector collector;
    public static final Fields boltFields = new Fields("current_url", "start_url", "date", "depth");

    // crawler4j
    CrawlConfig config;
    PageFetcher pageFetcher;
    RobotstxtConfig robotstxtConfig;

    RobotstxtServer robotstxtServer;
    Parser parser;

    boolean stayOnDomain;
    int throttlePauseMs;

    // Tracks the last hit crawl time per domain
    transient static final ConcurrentMap<String, Long> niceTracker = new MapMaker().makeMap();

    // Tracks the pages we crawled so we don't re-crawl
    transient static final Cache<String, String> pageTracker =
            CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).initialCapacity(100000)
                .build();


    //Types of links and files we don't want to crawl
    static Pattern FILTERS[] = new Pattern[] {
            Pattern.compile("\\.(ico|css|sit|eps|wmf|zip|ppt|mpg|xls|gz|rpm|tgz|mov|exe|bmp|js)$"),
            Pattern.compile("[\\?\\*\\!\\@\\=]"),
            Pattern.compile("^(file|ftp|mailto):"),
            Pattern.compile(".*(/[^/]+)/[^/]+\1/[^/]+\1/")
    };

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector)
    {
        this.stormConfig = (Map<String,String>) map;
        this.collector = collector;

        stayOnDomain = Boolean.valueOf(stormConfig.get("stay.on.domain"));
        throttlePauseMs = Integer.valueOf(stormConfig.get("throttle.pause.ms"));

        //Setup crawler4j
        config = new CrawlConfig();
        config.setIncludeHttpsPages(true);
        pageFetcher = new PageFetcher(config);
        robotstxtConfig = new RobotstxtConfig();
        robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
        parser = new Parser(config);
    }

    @Override
    public void execute(Tuple input)
    {
        String currentUrl = input.getStringByField("current_url");
        String startUrl = input.getStringByField("start_url");
        Date date = (Date)input.getValueByField("date");
        Integer depth = input.getIntegerByField("depth");


        WebURL curURL = new WebURL();
        curURL.setURL(URLCanonicalizer.getCanonicalURL(currentUrl));

        WebURL baseURL = new WebURL();
        baseURL.setURL(URLCanonicalizer.getCanonicalURL(startUrl));

        //Stay on start domain if configured
        if (stayOnDomain && !baseURL.getDomain().equals(curURL.getDomain()))
        {
            logger.error("no longer on base domain {} -> {} ", baseURL, curURL);
            collector.ack(input);
            return;
        }

        if (pageTracker.getIfPresent(curURL.getURL()) != null && (!curURL.getPath().equals("/") && depth != 0))
        {
            logger.info("Already visited page {}", currentUrl);
            collector.ack(input);
            return;
        }

        //See if page is in out filter list
        if (filtered(curURL))
        {
            logger.info("Filtered {}", currentUrl);
            collector.ack(input);
            return;
        }

        //Be a good web citizen
        if (!robotstxtServer.allows(curURL))
        {
            logger.warn("robots denied access to {}", currentUrl);
            collector.ack(input);
            return;
        }

        // Part of being a good web citizen is to not crawl a site
        // as fast as you can
        //
        // Check if we should slow down
        Long lastCrawl = niceTracker.get(curURL.getDomain());
        long now = System.currentTimeMillis();
        if (lastCrawl != null && (now - lastCrawl) < throttlePauseMs)
        {
            logger.info("Slowing down crawler to {}, sleeping for {}ms", curURL.getDomain(), now - lastCrawl);
            Utils.sleep(now - lastCrawl);
        }

        logger.info("crawling: {} , depth {}", currentUrl, depth);

        PageFetchResult fetchResult = null;
        try
        {
            fetchResult = pageFetcher.fetchHeader(curURL);
            int statusCode = fetchResult.getStatusCode();

            // TODO: Mark bad urls so we don't keep trying to re-crawl
            if (statusCode != HttpStatus.SC_OK) {
                if (statusCode == HttpStatus.SC_MOVED_PERMANENTLY || statusCode == HttpStatus.SC_MOVED_TEMPORARILY) {
                    String movedToUrl = fetchResult.getMovedToUrl();
                    if (movedToUrl == null) {
                        logger.warn("Page was moved, but no idea where");
                        collector.ack(input);
                        return;
                    }
                }
                else if (fetchResult.getStatusCode() == CustomFetchStatus.PageTooBig)
                {
                    logger.info("Skipping a page which was bigger than max allowed size: " + curURL.getURL());
                    collector.ack(input);
                    return;
                }
                else
                {
                    logger.info("{} status was {}", currentUrl, statusCode);
                }
            }

            //Were we redirected?
            if (!curURL.getURL().equals(fetchResult.getFetchedUrl()))
            {
                logger.info("redirected from {} to {} ", currentUrl, fetchResult.getFetchedUrl());
            }

            Page page = new Page(curURL);
            boolean fetched = fetchResult.fetchContent(page);
            niceTracker.put(curURL.getDomain(), System.currentTimeMillis());
            pageTracker.put(curURL.getURL(),"hit");

            //Pull and parse page
            if (fetched && parser.parse(page, curURL.getURL()))
            {
                ParseData parseData = page.getParseData();
                if (parseData instanceof HtmlParseData)
                {
                    HtmlParseData htmlParseData = (HtmlParseData) parseData;
                    for (WebURL webURL : htmlParseData.getOutgoingUrls())
                    {
                        //Should we continue crawling this site?
                        if (depth > 0 && (!stayOnDomain || baseURL.getDomain().equals(webURL.getDomain())))
                        {
                            //Don't waste time re-crawling pages we recently processed.
                            if (pageTracker.getIfPresent(webURL.getURL()) == null)
                            {
                                //This is a recursive bolt.
                                collector.emit("scraper_stream", input, new Values(webURL.getURL(), baseURL.getURL(), date, depth - 1));
                            }
                        }

                        //Emit Links
                        collector.emit("outgoinglinks_stream", input,
                                new Values(Sets.newHashSet(webURL.getURL()), curURL.getURL(), date));

                        collector.emit("incominglinks_stream", input,
                                new Values(Sets.newHashSet(curURL.getURL()), webURL.getURL(), date));
                    }

                    //Emit HTML Content
                    collector.emit("contents_stream", input,
                            new Values(htmlParseData.getTitle(), new String(page.getContentData(), "UTF-8"), curURL.getURL(), date));
                }

                //if we wanted to do something with images
                if (page.getContentType() != null && page.getContentType().contains("image")) {

                }

                //if we wanted to do something with css
                if (page.getContentType() != null && page.getContentType().contains("css")) {

                }
            }
            collector.ack(input);
            logger.info("finished url: {}",curURL.getURL());
        }
        catch (Throwable e)
        {
            logger.error(e.getMessage() + ", while processing: " + curURL.getURL());
            collector.fail(input);
            collector.reportError(e);
        }
    }

    private boolean filtered(WebURL curURL)
    {
        for (Pattern fp : FILTERS)
        {
            if (fp.matcher(curURL.getURL()).find())
                return true;
        }

        return false;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declareStream("scraper_stream", boltFields);
        declarer.declareStream("outgoinglinks_stream", new Fields("outgoing_url", "url", "date"));
        declarer.declareStream("incominglinks_stream", new Fields("incoming_url", "url", "date"));
        declarer.declareStream("contents_stream", new Fields("title", "html", "url", "date"));
    }

}
