package com.github.tjake.stormscraper.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.ArticleSentencesExtractor;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.StringReader;
import java.util.Map;


public class ContentExtractionBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(ContentExtractionBolt.class);

    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
       this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple input)
    {
        String url = input.getStringByField("url");
        String html = input.getStringByField("html");
        Object date = input.getValueByField("date");

        if (html == null)
        {
            logger.error("No content for : {}", url);
            collector.ack(input);
            return;
        }

        try
        {
            TextDocument td = new BoilerpipeSAXInput(new InputSource(
                    new StringReader(html))).getTextDocument();

            ArticleSentencesExtractor.INSTANCE.process(td);

            collector.emit(input, new Values(td.getContent(), url, date));
            collector.ack(input);

            logger.info("extracted text for {}", url);
        }
        catch (Exception e)
        {
            collector.fail(input);
            logger.error("error extracting text from {} {}", url, e);
            collector.reportError(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("text", "url", "date"));
    }

}
