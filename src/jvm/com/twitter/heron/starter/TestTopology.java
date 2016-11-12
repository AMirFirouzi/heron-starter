package com.twitter.heron.starter;

import org.apache.storm.testing.TestWordSpout;
import com.twitter.heron.starter.bolt.redisReportBolt;
import com.twitter.heron.starter.spout.BookWordSpout;
import org.apache.storm.utils.Utils;
import com.twitter.heron.starter.bolt.CountBolt;
import com.twitter.heron.starter.bolt.ReportBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.Graph;

/**
 * This topology demonstrates how to count distinct words from
 * a stream of words.
 *
 * This is an example for org.apache Real Time Analytics Course - ud381
 *
 */
public class TestTopology {

    /**
     * Constructor - does nothing
     */
    private TestTopology() { }

    public static void main(String[] args) throws Exception
    {
        Graph g =new Graph();

        // create the topology
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("c1s1", new BookWordSpout(), 2);

        builder.setSpout("c2s2", new BookWordSpout(), 2);

        // attach the count bolt using fields grouping - parallelism of 15
        builder.setBolt("c3b1", new CountBolt(), 3)
                .fieldsGrouping("c1s1", new Fields("word"))
                .fieldsGrouping("c2s2", new Fields("word"));

        builder.setBolt("c4b2", new CountBolt(), 3)
                .fieldsGrouping("c1s1", new Fields("word"));

        builder.setBolt("c5b3", new ReportBolt(), 3)
                .globalGrouping("c3b1")
                .globalGrouping("c4b2");

        // create the default config object
        Config conf = new Config();

        // set the config in debugging mode
        conf.setDebug(true);


        if (args != null && args.length > 0) {
            // run it in a live cluster

            // set the number of workers for running all spout and bolt tasks

            conf.setNumWorkers(3);

            StormSubmitter.submitTopology("BookWordCountTopology", conf, builder.createTopology());
        } else {
            // run it in a simulated local cluster

            // set the number of threads to run - similar to setting number of workers in live cluster
            //conf.setMaxTaskParallelism(3);

            // create the local cluster instance
            LocalCluster cluster = new LocalCluster();
            // submit the topology to the local cluster
            cluster.submitTopology("BookWordCountTopology", conf, builder.createTopology());

            //**********************************************************************
            // let the topology run for 30 seconds. note topologies never terminate!
            Utils.sleep(300000);
            cluster.killTopology("BookWordCountTopology");

            // we are done, so shutdown the local cluster
            cluster.shutdown();
        }
    }
}
