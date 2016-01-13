package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import io.KGS;
import io.Parameters;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Roy Gao on 1/9/2016.
 */
public class DBolt implements IRichBolt {

	OutputCollector _collector;
	String ID;

	private Jedis jedis;

	private Map<Integer, KGS> infoList = new HashMap<>();

	public DBolt(String host, int port) {
		jedis = new Jedis(host, port);
	}

	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		ID = context.getThisComponentId();
	}

	@Override
	public void execute(Tuple tuple) {
		if (jedis.exists(Parameters.REDIS_SUM + ID)) {
			// emit sum to Controller

		} else if (jedis.exists(Parameters.REDIS_DETAIL + ID)) {
			// emit detail to Controller

		} else {
			// record kgs info and put pressure
			int key = (int) tuple.getValue(0);
			int g = (int) tuple.getValue(1);

			recordInfo(key, g, 1);
			for (int i = 0; i < g; ++i)
				calculatePi();
		}

		_collector.ack(tuple);
	}

	private void recordInfo(int key, int g, int s) {
		if (!infoList.containsKey(key))
			infoList.put(key, new KGS(key, g, s));
		else {
			infoList.get(key).addG(g);
			infoList.get(key).addS(s);
		}
	}

	private double calculatePi() {
		double pi = 0;
		for (int n = 0; n <= 5000; ++n)
			pi += (n % 2 == 0 ? 1 : -1) / (2 * n + 1);
		return pi;
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
