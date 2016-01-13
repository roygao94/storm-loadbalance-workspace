package spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import io.Parameters;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by Roy Gao on 1/9/2016.
 */
public class RedisQueueSpout extends BaseRichSpout {

	public static final String OUTPUT_FIELD = "text";
	protected SpoutOutputCollector _collector;

//	private String queue;
	private String host;
	private int port;
	private long len = 0;
	private static long count = 0;
	private transient Jedis jedis = null;

	public RedisQueueSpout(String host, int port) {
		this.host = host;
		this.port = port;
//		this.queue = queue;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(OUTPUT_FIELD));
	}

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		_collector = spoutOutputCollector;
	}

	@Override
	public void nextTuple() {
		Jedis jedis = getConnectedJedis();
		if (jedis == null)
			return;

		Object text = null;
		try {
			text = jedis.lindex(Parameters.REDIS_KGS, count);
			if (++count >= len)
				count = 0;
		} catch (Exception e) {
			disconnect();
		}

		if (text != null)
			emitData(text);
	}

	private Jedis getConnectedJedis() {
		if (jedis != null) {
			return jedis;
		}
		//try connect to redis server
		try {
			jedis = new Jedis(host, port);
			len = jedis.llen(Parameters.REDIS_KGS);
		} catch (Exception e) {
		}
		return jedis;
	}

	public void emitData(Object data) {
		_collector.emit(Arrays.asList(data), data);
	}

	private void disconnect() {
		try {
			jedis.disconnect();
		} catch (Exception e) {
		}

		jedis = null;
	}
}
