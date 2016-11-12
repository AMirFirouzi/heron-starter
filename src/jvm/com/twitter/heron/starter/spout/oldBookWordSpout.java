package com.twitter.heron.starter.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;


/**
 * A spout that emits a random word
 */
public class oldBookWordSpout extends BaseRichSpout {

    // Random number generator
    private Random rnd;


    // To output tuples from spout to the next stage
    private SpoutOutputCollector collector;

    // For storing the list of words to be fed into the topology
    //private String[] wordList;
    private ArrayList<String> wordList;

    @Override
    public void open(
            Map                     map,
            TopologyContext         topologyContext,
            SpoutOutputCollector    spoutOutputCollector) {

        // initialize the random number generator
        rnd = new Random(31);

        // save the output collector for emitting tuples
        collector = spoutOutputCollector;

        // initialize a set of words
        //wordList = new String[]{""};


        File file = new File("book.txt");
        Scanner scanFile = null;
        try {
            scanFile = new Scanner(new FileReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        wordList = new ArrayList<String>();
        String theWord;
        Integer i = 0;
        //nextInt=0;
        while (scanFile.hasNext()) {
            theWord = scanFile.next();
            wordList.add(theWord);
        }
    }

    //private Integer nextInt;
    @Override
    public void nextTuple()
    {
        // sleep a second before emitting any word
        Utils.sleep(1000);

        // generate a random number based on the wordList length
        int nextInt = rnd.nextInt(wordList.size());

        // emit the word chosen by the random number from wordList
        collector.emit(new Values(wordList.get(nextInt++)));
    }

    @Override
    public void declareOutputFields(
            OutputFieldsDeclarer outputFieldsDeclarer)
    {
        // tell storm the schema of the output tuple for this spout
        // tuple consists of a single column called 'word'
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}