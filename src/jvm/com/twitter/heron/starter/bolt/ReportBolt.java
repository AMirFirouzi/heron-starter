package com.twitter.heron.starter.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * A bolt that prints the word and count to redis
 */
public class ReportBolt extends BaseRichBolt
{
    // place holder to keep the connection to redis
    //transient RedisConnection<String,String> redis;
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
        // instantiate a redis connection
        //RedisClient client = new RedisClient("localhost",6379);

        // initiate the actual connection
        //redis = client.connect();

      try {
        writer = new PrintWriter("/home/amir/Projects/git/heron-examples/heron-starter/output.txt", "UTF-8");
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

        // access the second column 'count'
        int count = tuple.getIntegerByField("count");

        // publish the word count to redis using word as the key
        //redis.publish("WordCountTopology", word + "|" + Long.toString(count));
        //collector.emit(tuple, new Values(tuple.getStringByField("word")));
        //writer.println(word+", "+count);
        //writer.close();
        collector.ack(tuple);

        System.out.println("component=>"+compId+"-"+taskId+" - word=>"+word + ", " + "count=>"+count);
        writer.println("component=>"+compId+"-"+taskId+" - word=>"+word + ", " + "count=>"+count);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // nothing to add - since it is the final bolt
    }
}