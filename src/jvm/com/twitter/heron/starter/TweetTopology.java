package com.twitter.heron.starter;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import com.twitter.heron.starter.spout.TwitterSampleSpout;

/**
 * This is a basic example of a Storm topology.
 */
public class TweetTopology {

  public static class StdoutBolt extends BaseRichBolt {
    OutputCollector _collector;
    String taskName;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      taskName = context.getThisComponentId() + "_" + context.getThisTaskId();
    }

    @Override
    public void execute(Tuple tuple) {
      System.out.println(tuple.getValue(0));
      _collector.emit(tuple, new Values(tuple.getValue(0)));
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", 
           new TwitterSampleSpout("8I9018T1gdWDLihXddAcSvg2E",
                 "XMtgBK2TX10tCpe6fAXdnwwo8yGJHshlkVk2qX4Yhu4fH4Aunq",
                 "303802154-pNmMuEovbQXmIdofz4sm2ELvBUya68rCguib2HL4",
                 "NFXa4A16XlTQ7hVfrLeiUA8yyh7Jd0abgTcGSK0C1Lw7g"), 1);

    builder.setBolt("stdout", new StdoutBolt(), 3).shuffleGrouping("word");

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(100000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
