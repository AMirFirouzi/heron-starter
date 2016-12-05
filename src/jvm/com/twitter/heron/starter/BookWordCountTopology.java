package com.twitter.heron.starter;

import com.twitter.heron.api.graph.Graph;
import com.twitter.heron.common.basics.Constants;
import com.twitter.heron.starter.bolt.CountBolt;
import com.twitter.heron.starter.bolt.ReportBolt;
import com.twitter.heron.starter.bolt.redisReportBolt;
import com.twitter.heron.starter.spout.BookWordSpout;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * This topology demonstrates how to count distinct words from
 * a stream of words.
 * <p>
 * This is an example for org.apache Real Time Analytics Course - ud381
 */
public class BookWordCountTopology {

  /**
   * Constructor - does nothing
   */
  private BookWordCountTopology() {
  }

  public static void main(String[] args) throws Exception {

    Graph G = new Graph();
//    G.addEdge("A", "B").addWeights(new String[]{"1"});
//    G.addEdge("A", "C").addWeights(new String[]{"11"});
//    G.addEdge("C", "D").addWeights(new String[]{"3"});;
//    G.addEdge("D", "E").addWeights(new String[]{"5"});;
//    G.addEdge("D", "G").addWeights(new String[]{"2"});;
//    G.addEdge("E", "G").addWeights(new String[]{"8"});;
//
//    G.getVertex("A").addWeights(new String[]{"1","1","3"});
//    G.getVertex("B").addWeights(new String[]{"5","2","4"});
//    G.getVertex("C").addWeights(new String[]{"3","3","6"});
//    G.getVertex("D").addWeights(new String[]{"2","5","3"});
//    G.getVertex("E").addWeights(new String[]{"6","4","1"});
//    G.getVertex("G").addWeights(new String[]{"3","2","3"});
//
//    // print out graph again by iterating over vertices and edges
//    System.out.println(G.toString());

    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    // attach the word spout to the topology - parallelism of 5
    builder.setSpout("s1", new BookWordSpout(), 3);

    // attach the count bolt using fields grouping - parallelism of 15
    builder.setBolt("b1", new CountBolt(), 2).fieldsGrouping("s1", new Fields("word"));

    builder.setBolt("b2", new ReportBolt(), 1).globalGrouping("b1");

    // create the default config object
    Config conf = new Config();
    // set the config in debugging mode
    conf.setDebug(true);
//    conf.setContainerMaxRamHint(10L * Constants.);
//    conf.setMaxSpoutPending(100);
//    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");
//    conf.setComponentJvmOptions("s1", "-agentlib:jdwp=transport=dt_socket,address=8888,server=y,suspend=n");
//    conf.setComponentJvmOptions("b1", "-agentlib:jdwp=transport=dt_socket,address=8888,server=y,suspend=n");
//    conf.setComponentJvmOptions("b2", "-agentlib:jdwp=transport=dt_socket,address=8888,server=y,suspend=n");

    if (args != null && args.length > 0) {
      // run it in a live cluster

      // set the number of workers for running all spout and bolt tasks

      conf.setNumWorkers(2);

      StormSubmitter.submitTopology("BookWordCountTopology", conf, builder.createTopology());
    } else {
      // run it in a simulated local cluster

      // set the number of threads to run - similar to setting number of workers in live cluster
      conf.setMaxTaskParallelism(3);

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
