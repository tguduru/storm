package org.storm.bolts;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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
    private Integer id;
    private String name;
    private Map<String, Integer> counters;
    private OutputCollector outputCollector;

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepare(@SuppressWarnings("rawtypes") final Map stormConf, final TopologyContext context,
            final OutputCollector collector) {
        this.counters = new HashMap<String, Integer>();
        this.outputCollector = collector;
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(final Tuple input) {
        final String string = input.getString(0);
        if (counters.containsKey(string)) {
            final Integer c = counters.get(string) + 1;
            counters.put(string, c);
        } else {
            counters.put(string, 1);
        }
        outputCollector.ack(input);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup() {
        System.out.println("-- Words Counter  [" + name + " - " + id + "] --");
        for (final Entry<String, Integer> entry : counters.entrySet()) {
            System.out.println("-- " + entry.getKey() + " " + entry.getValue() + " --");
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
