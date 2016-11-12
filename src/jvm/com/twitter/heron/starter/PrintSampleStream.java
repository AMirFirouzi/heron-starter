// to use this example, uncomment the twitter4j dependency information in the project.clj,
// uncomment TwitterSampleSpout, and uncomment this class

package com.twitter.heron.starter;

import com.twitter.heron.starter.bolt.PrinterBolt;
import com.twitter.heron.starter.spout.TwitterSampleSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;


public class PrintSampleStream {        
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("spout", new TwitterSampleSpout("[Your customer key]",
            "[Your secret key]",
            "[Your access token]",
            "[Your access secret]"));
        builder.setBolt("print", new PrinterBolt())
                .shuffleGrouping("spout");

        Config conf = new Config();
        
        
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("test", conf, builder.createTopology());
        
        Utils.sleep(10000);
        cluster.shutdown();
    }
}
