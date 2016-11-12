package com.twitter.heron.starter;

import org.apache.storm.graph.Graph;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.utils.Utils;
import com.twitter.heron.starter.bolt.CountBolt;
import com.twitter.heron.starter.bolt.ReportBolt;
import com.twitter.heron.starter.bolt.redisReportBolt;
import com.twitter.heron.starter.spout.BookWordSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * This topology demonstrates how to count distinct words from
 * a stream of words.
 *
 * This is an example for org.apache Real Time Analytics Course - ud381
 *
 */
public class BookWordCountTopology {

  /**
   * Constructor - does nothing
   */
  private BookWordCountTopology() { }

  public static void main(String[] args) throws Exception
  {

    Graph G = new Graph();
    G.addEdge("A", "B").addWeights(new String[]{"1"});
    G.addEdge("A", "C").addWeights(new String[]{"11"});
    G.addEdge("C", "D").addWeights(new String[]{"3"});;
    G.addEdge("D", "E").addWeights(new String[]{"5"});;
    G.addEdge("D", "G").addWeights(new String[]{"2"});;
    G.addEdge("E", "G").addWeights(new String[]{"8"});;

    G.getVertex("A").addWeights(new String[]{"1","1","3"});
    G.getVertex("B").addWeights(new String[]{"5","2","4"});
    G.getVertex("C").addWeights(new String[]{"3","3","6"});
    G.getVertex("D").addWeights(new String[]{"2","5","3"});
    G.getVertex("E").addWeights(new String[]{"6","4","1"});
    G.getVertex("G").addWeights(new String[]{"3","2","3"});

    // print out graph again by iterating over vertices and edges
    System.out.println(G.toString());

    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    // attach the word spout to the topology - parallelism of 5
    builder.setSpout("word-spout", new BookWordSpout(), 10);

    // attach the count bolt using fields grouping - parallelism of 15
    builder.setBolt("count-bolt", new CountBolt(), 6).fieldsGrouping("word-spout", new Fields("word"));

    // attach the report bolt using global grouping - parallelism of 1
    //***************************************************
    // BEGIN YOUR CODE - part 2

    builder.setBolt("report-bolt", new redisReportBolt(), 3).globalGrouping("count-bolt");


    // END YOUR CODE
    //***************************************************

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
