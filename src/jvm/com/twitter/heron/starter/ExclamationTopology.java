package com.twitter.heron.starter;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology {

  public static class ExclamationBolt extends BaseRichBolt {
    OutputCollector _collector;
    String myId;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      myId = context.getThisComponentId();
    }

    @Override
    public void execute(Tuple tuple) {
      String msg = tuple.getString(0) + "!!!";
      _collector.emit(tuple, new Values(msg));
      _collector.ack(tuple);
      System.out.println(myId + ": " + msg);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  public static class ReportBolt extends BaseRichBolt
  {
    PrintWriter writer;
    OutputCollector collector;
    String compId,taskId;
    @Override
    public void prepare(
            Map map,
            TopologyContext topologyContext,
            OutputCollector         outputCollector)
    {
      collector = outputCollector;
      compId = topologyContext.getThisComponentId();
      taskId = topologyContext.getThisTaskId()+"";
      try {
        writer = new PrintWriter("/home/amir/Projects/output.txt", "UTF-8");
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void execute(Tuple tuple)
    {

      // access the first column 'word'
      String word = tuple.getStringByField("word");

      collector.ack(tuple);

      System.out.println("component=>"+compId+"-"+taskId+" - word=>"+word);
      writer.println("component=>"+compId+"-"+taskId+" - word=>"+word);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
      // nothing to add - since it is the final bolt
    }
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new TestWordSpout(), 10);
    builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("word");
    builder.setBolt("report", new ReportBolt(), 2).shuffleGrouping("exclaim1");

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(3000000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
