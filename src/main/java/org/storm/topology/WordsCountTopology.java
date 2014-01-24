package org.storm.topology;

import org.storm.bolts.WordCounterBolt;
import org.storm.bolts.WordNormalizerBolt;
import org.storm.spouts.WordReaderSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Topology which counts the words in a given text file
 * @author Guduru, Thirupathi Reddy
 */
public class WordsCountTopology {

    /**
     * @param args
     */
    @SuppressWarnings("javadoc")
    public static void main(final String[] args) {

        // Build the topology
        final TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("words-spout", new WordReaderSpout());
        topologyBuilder.setBolt("word-normalizer", new WordNormalizerBolt()).shuffleGrouping("words-spout");
        topologyBuilder.setBolt("words-count", new WordCounterBolt()).fieldsGrouping("word-normalizer",
                new Fields("word"));

        // Config setup
        final Config config = new Config();
        config.put("file", "src/main/resources/wordsFile.txt");
        config.setDebug(false);

        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        // Run the topology
        final LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("Words-Topology", config, topologyBuilder.createTopology());
        // wait for topology to finish the work
        try {
            // wait to complete all data processing
            Thread.sleep(5000);
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
        // kill the topology cluster
        localCluster.shutdown();
    }

}
