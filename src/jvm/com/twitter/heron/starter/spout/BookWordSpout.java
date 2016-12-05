package com.twitter.heron.starter.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

/**
 * A spout that emits a random word
 */
public class BookWordSpout extends BaseRichSpout {

  // To output tuples from spout to the next stage
  SpoutOutputCollector collector;
  private ArrayList<String> wordList;
  private ArrayList<String> randList;
  private FileReader fileReader;
  BufferedReader reader;
  File file;
  String[] words;
  Integer rand;



  public void open(
          Map                     map,
          TopologyContext         topologyContext,
          SpoutOutputCollector    spoutOutputCollector) {

    // save the output collector for emitting tuples
    collector = spoutOutputCollector;

    String filename = "/home/amir/Projects/heron/myheron/heron-starter/book.txt";
    try {
      file = new File(filename);
      this.fileReader = new FileReader(file);

      //scanner = new Scanner(file);
      //scanFile = new Scanner(new FileReader(file));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

    String str;
    reader = new BufferedReader(fileReader);
    wordList = new ArrayList<String>();
    randList = new ArrayList<String>();

    try {
      // Read all lines
      while ((str = reader.readLine()) != null) {
        if (str.trim().isEmpty()) {
          continue;
        }
        words = str.split("\\s+");
        for (int i = 0; i < words.length; i++) {
          words[i] = words[i].replaceAll("[^\\w]", "");

          if(words[i]!="" && (!wordList.contains(words[i]))){
            wordList.add(words[i]);
            //this.collector.emit(new Values(words[i]), words[i]);
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error reading tuple", e);
    } finally {
      completed = true;
    }
  }

  public void close() {
    try {
      fileReader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  boolean completed;
  //private Integer nextInt;

  public void nextTuple()
  {
    rand = new Random().nextInt(wordList.size()-1);

    while(randList.contains(rand)){
      rand = new Random().nextInt(wordList.size()-1);
    }

    randList.add(rand+"");
    String word = wordList.get(rand);
    this.collector.emit(new Values(new Object[]{word}));

  }

  @Override
  public void declareOutputFields(
          OutputFieldsDeclarer outputFieldsDeclarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a single column called 'word'
    outputFieldsDeclarer.declare(new Fields(new String[]{"word"}));
  }
}