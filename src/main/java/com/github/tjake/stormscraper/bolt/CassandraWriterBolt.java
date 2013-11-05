package com.github.tjake.stormscraper.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.*;
import com.google.common.base.Preconditions;
import org.apache.commons.cli.MissingArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CassandraWriterBolt extends BaseRichBolt {

    static final Logger logger = LoggerFactory.getLogger(CassandraWriterBolt.class);
    OutputCollector outputCollector;
    Cluster cassandraCluster;
    Session session;
    String component;
    Map<String,String> config;
    String cql;

    public static Session getSessionWithRetry(Cluster cluster, String keyspace) {
        while (true) {
            try {
                return cluster.connect(keyspace);
            } catch (NoHostAvailableException e) {
                logger.warn("All Cassandra Hosts offline. Waiting to try again.");
                Utils.sleep(1000);
            }
        }

    }

    public static Cluster setupCassandraClient(String []nodes) {
        return Cluster.builder()
                .withoutJMXReporting()
                .withoutMetrics()
                .addContactPoints(nodes)
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .withReconnectionPolicy(new ExponentialReconnectionPolicy(100L, TimeUnit.MINUTES.toMillis(5)))
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                .build();
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        config = (Map<String,String>) map;
        cassandraCluster = setupCassandraClient(config.get("cassandra.nodes").split(","));
        session = CassandraWriterBolt.getSessionWithRetry(cassandraCluster,config.get("cassandra.keyspace"));



        //We encode the cql for this bolt in the config under the component name
        component = topologyContext.getThisComponentId();
        cql = config.get(component+".cql");

        Preconditions.checkArgument(cql != null, "CassandraWriterBolt:"+component+" is missing a cql statement in bolt config");
    }

    @Override
    public void execute(Tuple input) {

        try {

            PreparedStatement stmt = session.prepare(cql);
            BoundStatement bound = stmt.bind(input.getValues().toArray());

            session.execute(bound);
            outputCollector.ack(input);
            //logger.info("Wrote to cassandra info about {}",component);

        } catch (Throwable t) {

            outputCollector.reportError(t);
            outputCollector.fail(input);

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //No outputs
    }
}
