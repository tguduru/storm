package org.storm.spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * Word reader topology spout to read lines and send them to bolts for word counts app.
 * @author Guduru, Thirupathi Reddy
 */
public class WordReaderSpout implements IRichSpout {

    /**
     * The serialVersionUID.
     */
    private static final long serialVersionUID = 3176783787478077416L;

    private SpoutOutputCollector spoutOutputCollector;
    private FileReader fileReader;
    private boolean completed = false;
    private TopologyContext topologyContext;

    /**
     * Is the topology running in distributed mode/local mode
     * @return boolean , return false as running in local mode only
     */
    public boolean isDistributed() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open(@SuppressWarnings("rawtypes") final Map conf, final TopologyContext context,
            final SpoutOutputCollector collector) {
        try {
            this.topologyContext = context;
            this.fileReader = new FileReader(conf.get("file").toString());
        } catch (final FileNotFoundException ex) {
            throw new RuntimeException("Error reading file", ex);
        }
        this.spoutOutputCollector = collector;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void activate() {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deactivate() {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void nextTuple() {
        if (completed) {
            try {
                Thread.sleep(10);
            } catch (final InterruptedException ex) {
                // swallow exception
            }
        }
        String nextLine;
        final BufferedReader bufferedReader = new BufferedReader(fileReader);
        try {
            while ((nextLine = bufferedReader.readLine()) != null) {
                this.spoutOutputCollector.emit(new Values(nextLine), nextLine);
            }

        } catch (final Exception ex) {
            throw new RuntimeException("Error reading tuple", ex);
        } finally {
            completed = true;
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void ack(final Object msgId) {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fail(final Object msgId) {
        System.out.println("FAILED : " + msgId);
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
        return null;
    }

}
