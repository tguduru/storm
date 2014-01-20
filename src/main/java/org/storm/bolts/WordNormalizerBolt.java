package org.storm.bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * Bolt which will normalize the line into words, convert them to small case and remove leading/trailing spaces
 * @author Guduru, Thirupathi Reddy
 */
public class WordNormalizerBolt implements IRichBolt {

    /**
     * The serialVersionUID.
     */
    private static final long serialVersionUID = 2946013645374919830L;

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepare(@SuppressWarnings("rawtypes") final Map stormConf, final TopologyContext context,
            final OutputCollector collector) {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(final Tuple input) {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

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
        // TODO Auto-generated method stub
        return null;
    }

}
