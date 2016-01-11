package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by Roy Gao on 1/9/2016.
 */
public class DBolt implements IRichBolt {

	OutputCollector _collector;

	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		int key = (int) tuple.getValue(0);
		int g = (int) tuple.getValue(1);

		for (int i = 0; i < g; ++i)
			for (int j = 0; j < 100000; ++j) ;

		_collector.ack(tuple);
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "g"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
