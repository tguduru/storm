package org.storm.bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Bolt which will normalize the line into words, convert them to small case and remove leading/trailing spaces
 * @author Guduru, Thirupathi Reddy
 */
public class WordNormalizerBolt implements IRichBolt {

    /**
     * The serialVersionUID.
     */
    private static final long serialVersionUID = 2946013645374919830L;
    private OutputCollector outputCollector;

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepare(@SuppressWarnings("rawtypes") final Map stormConf, final TopologyContext context,
            final OutputCollector collector) {
        this.outputCollector = collector;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(final Tuple input) {
        final String sentence = input.getString(0);
        final String[] words = sentence.split(" ");
        for (String word : words) {
            word = word.trim();
            if (!word.isEmpty()) {
                word = word.toLowerCase();
                outputCollector.emit(new Values(word));
            }
        }
        outputCollector.ack(input);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
