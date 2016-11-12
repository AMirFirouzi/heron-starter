package com.twitter.heron.starter.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * A bolt that counts the words that it receives
 */
public class CountBolt extends BaseRichBolt {

    // To output tuples from this bolt to the next stage bolts, if any
    private OutputCollector collector;

    // Map to store the count of the words
    private Map<String, Integer> countMap;

    String compId,taskId;
    @Override
    public void prepare(
            Map                     map,
            TopologyContext topologyContext,
            OutputCollector         outputCollector)
    {

        // save the collector for emitting tuples
        collector = outputCollector;
        compId = topologyContext.getThisComponentId();
        taskId = topologyContext.getThisTaskId()+"";
        // create and initialize the map
        countMap = new HashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple tuple)
    {
        Integer val=0;
        //**************************************************
        //BEGIN YOUR CODE - Part 1a
        //Check if incoming word is in countMap.  If word does not
        //exist then add word with count = 1, if word exist then
        //increment count.

        //Syntax to get the word from the 1st column of incoming tuple
        String word = tuple.getString(0).toLowerCase();

        // check if the word is present in the map
        if (countMap.get(word) == null) {

            // not present, add the word with a count of 1
            countMap.put(word, 1);
        } else {

            // already there, hence get the count
            val = countMap.get(word);

            // increment the count and save it to the map
            countMap.put(word, ++val);
        }

        //After countMap is updated, emit word and count to output collector
        // Syntax to emit the word and count (uncomment to emit)
        collector.emit(tuple,new Values(word, countMap.get(word)));
        //System.out.println("component=>"+compId+"-"+taskId+" - word=>"+word + ", " + "count=>"+val);
        collector.ack(tuple);
        //END YOUR CODE
        //***************************************************
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        // tell storm the schema of the output tuple for this spout
        // tuple consists of a two columns called 'word' and 'count'

        // declare the first column 'word', second colmun 'count'

        //****************************************************
        //BEGIN YOUR CODE - part 1b
        //uncomment line below to declare output

        outputFieldsDeclarer.declare(new Fields("word","count"));

        //END YOUR CODE
        //****************************************************
    }
}