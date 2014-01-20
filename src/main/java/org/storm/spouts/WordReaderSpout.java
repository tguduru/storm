package org.storm.spouts;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

/**
 * Word reader topology spout to read lines and send them to bolts for word counts app.
 * @author Guduru, Thirupathi Reddy
 */
public class WordReaderSpout implements IRichSpout {

    /**
     * The serialVersionUID.
     */
    private static final long serialVersionUID = 3176783787478077416L;

    /**
     * {@inheritDoc}
     */
    @Override
    public void open(@SuppressWarnings("rawtypes") final Map conf, final TopologyContext context,
            final SpoutOutputCollector collector) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void activate() {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deactivate() {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void nextTuple() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void ack(final Object msgId) {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fail(final Object msgId) {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
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
