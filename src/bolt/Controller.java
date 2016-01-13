package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import io.Parameters;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * Created by Roy Gao on 1/13/2016.
 */
public class Controller implements IRichBolt {

	TopologyContext context;
	OutputCollector _collector;
	int DBoltNumber;

	private String host;
	private int port;
	private transient Jedis jedis;

	public Controller(String host, int port) {
		this.host = host;
		this.port = port;
	}

	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.context = context;
		_collector = collector;
		DBoltNumber = context.getComponentTasks("dbolt").size();
	}

	@Override
	public void execute(Tuple tuple) {
		Jedis jedis = getConnectedJedis();
		try {
			Thread.sleep(10000);

			for (int i = 0; i < DBoltNumber; ++i)
				jedis.lpush(Parameters.REDIS_SUM + context.getComponentTasks("dbolt").get(i));

		} catch (Exception e) {
		}
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

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
