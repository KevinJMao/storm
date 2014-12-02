package com.kevinmao.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import com.google.common.base.Throwables;
import com.kevinmao.graphite.GraphiteAdapter;
import com.kevinmao.graphite.GraphiteConnectionAttemptFailure;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

public abstract class GraphiteWriterBoltBase extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(GraphiteWriterBoltBase.class);
    public static final String GRAPHITE_PREFIX = "ddos.output";
    protected OutputCollector collector;
    private String graphiteServerHostname;
    private int graphiteServerPortNumber;
    private GraphiteAdapter graphiteAdapter;

    public GraphiteWriterBoltBase(String graphiteServerHostname, int graphiteServerPortNumber) {
        this.graphiteServerHostname = graphiteServerHostname;
        this.graphiteServerPortNumber = graphiteServerPortNumber;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    private void graphiteConnect() {
        graphiteAdapter = new GraphiteAdapter(new InetSocketAddress(graphiteServerHostname, graphiteServerPortNumber));
        try {
            graphiteAdapter.connect();
        }
        catch (GraphiteConnectionAttemptFailure e) {
            String trace = Throwables.getStackTraceAsString(e);
            LOG.error("Could not connect to Graphite server " + graphiteAdapter.serverFingerprint() + ": " + trace);
        }
    }

    private void graphiteDisconnect() {
        if (graphiteAdapter != null) {
            graphiteAdapter.disconnect();
        }
    }

    protected void sendToGraphite(String metricPath, String value, long timestamp) {
//        LOG.info("Attempting to send to Graphite: (" + metricPath + "," + value + "," + timestamp + ")");
        try {
            if (graphiteAdapter != null) {
                graphiteAdapter.send(metricPath, value, timestamp);
            }
        }
        catch (IOException e) {
            String trace = Throwables.getStackTraceAsString(e);
            String msg = "Could not send metrics update to Graphite server " + graphiteAdapter.serverFingerprint() + ": " + trace +
                    " (" + graphiteAdapter.getFailures() + " failed attempts so far)";
            LOG.error(msg);
        }
    }
}
