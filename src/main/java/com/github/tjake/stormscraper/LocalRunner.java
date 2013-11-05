package com.github.tjake.stormscraper;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.github.tjake.stormscraper.bolt.CassandraWriterBolt;
import com.github.tjake.stormscraper.bolt.ContentExtractionBolt;
import com.github.tjake.stormscraper.bolt.ScraperBolt;
import com.github.tjake.stormscraper.spout.ScraperSpout;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class LocalRunner {

    public static void main(String args[]) {

        //Creates local storm cluster
        LocalCluster cluster = new LocalCluster();

        //Load properties file
        Properties props = new Properties();
        try {
            InputStream is = LocalRunner.class.getClassLoader().getResourceAsStream("scraper.properties");

            if (is == null)
                throw new RuntimeException("Classpath missing scraper.properties file");

            props.load(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Config conf = new Config();
        conf.setDebug(true);

        //Copy properies to storm config
        for (String name : props.stringPropertyNames()) {
            conf.put(name, props.getProperty(name));
        }

        conf.setMaxTaskParallelism(Runtime.getRuntime().availableProcessors());
        conf.setDebug(false);

        cluster.submitTopology("storm-crawler", conf, createTopology());

    }

    static StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        // Emits the sites we want to scrape
        builder.setSpout("scraper_spout", new ScraperSpout(), 1);

        // Webpage Scraper
        builder.setBolt("scraper_bolt", new ScraperBolt(), 3)
                .shuffleGrouping("scraper_spout")
                .fieldsGrouping("scraper_bolt", "scraper_stream", new Fields("current_url"));

        // Text Extractor
        builder.setBolt("textextract_bolt", new ContentExtractionBolt(), 3)
                .shuffleGrouping("scraper_bolt", "contents_stream");


        // Cassandra Writers

        //Writes links from each page
        builder.setBolt("outgoinglinks_writer", new CassandraWriterBolt())
                .shuffleGrouping("scraper_bolt", "outgoinglinks_stream");

        builder.setBolt("incominglinks_writer", new CassandraWriterBolt())
                .shuffleGrouping("scraper_bolt", "outgoinglinks_stream");


        //Writes html for each page
        builder.setBolt("html_writer", new CassandraWriterBolt())
                .shuffleGrouping("scraper_bolt", "contents_stream");

        //Writes extracted text from each page
        builder.setBolt("text_writer", new CassandraWriterBolt())
                .shuffleGrouping("textextract_bolt");


        return builder.createTopology();
    }

}
