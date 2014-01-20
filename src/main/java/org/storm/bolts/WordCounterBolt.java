package org.storm.bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * Bolt that will count the number of words
 * @author Guduru, Thirupathi Reddy
 */
public class WordCounterBolt implements IRichBolt {

    /**
     * The serialVersionUID.
     */
    private static final long serialVersionUID = 1057910306200572216L;

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
        // TODO Auto-generated method stub

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
