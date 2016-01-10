package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Roy Gao on 1/9/2016.
 */
public class UBolt implements IRichBolt {

	Map<Integer, Integer> routingTable;
	TopologyContext context;
	OutputCollector collector;
	int taskNumber;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		routingTable = new HashMap<>();
		this.collector = collector;
		this.context = context;
		taskNumber = context.getComponentTasks("dbolt").size();
	}

	@Override
	public void execute(Tuple tuple) {
		String line = tuple.getString(0);
		String[] split = line.split(",");
		int key = Integer.parseInt(split[0]);
		int g = Integer.parseInt(split[1]);
		int taskID = context.getComponentTasks("dbolt").get(key % taskNumber);

		collector.emitDirect(taskID, tuple, new Values(key, g));
		collector.ack(tuple);
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
