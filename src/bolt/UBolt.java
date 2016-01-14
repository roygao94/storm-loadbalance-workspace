package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import io.Parameters;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Roy Gao on 1/9/2016.
 */
public class UBolt implements IRichBolt {

	TopologyContext context;
	OutputCollector _collector;
	String ID;
	int DBoltNumber;

	private String host;
	private int port;
	private transient Jedis jedis;

	Map<Integer, Integer> routingTable;

	public UBolt(String host, int port) {
		this.host = host;
		this.port = port;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		routingTable = new HashMap<>();
		this.context = context;
		_collector = collector;
		ID = context.getThisComponentId();
		DBoltNumber = context.getComponentTasks("d-bolt").size();
	}

	@Override
	public void execute(Tuple tuple) {
		Jedis jedis = getConnectedJedis();
		if (jedis.exists(Parameters.REDIS_RT + ID)) {
			// update routing table

		}

		String line = tuple.getString(0);
		String[] split = line.split(",");
		int key = Integer.parseInt(split[0]);
		int g = Integer.parseInt(split[1]);
		int taskID = context.getComponentTasks("d-bolt").get(routingTable.containsKey(key) ? routingTable.get(key) : key % DBoltNumber);

		_collector.emitDirect(taskID, tuple, new Values(key, g));
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

	private Jedis getConnectedJedis() {
		if (jedis != null)
			return jedis;

		try {
			jedis = new Jedis(host, port);
		} catch (Exception e) {
		}

		return jedis;
	}
}
