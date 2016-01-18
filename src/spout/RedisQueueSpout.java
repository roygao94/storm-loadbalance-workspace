package spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import conf.Parameters;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * Created by Roy Gao on 1/9/2016.
 */
public class RedisQueueSpout extends BaseRichSpout {

	public static final String OUTPUT_FIELD = "text";
	protected SpoutOutputCollector _collector;

	private Parameters parameters;
//	private String host;
//	private int port = Parameters.REDIS_PORT;
	private long len = 0;
	private static long count = 0;
	private transient Jedis jedis = null;

	private Queue<Integer> randomQueue;

	public RedisQueueSpout(Parameters parameters) {
		this.parameters = new Parameters(parameters);
		randomQueue = new LinkedList<>();
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

		Object text = getTextByOrder();
//		Object text = getRandomText();

		if (text != null)
			emitData(text);
	}

	private Object getTextByOrder() {
		Object text = null;
		text = jedis.lindex(Parameters.REDIS_KGS, count);
		if (++count >= len)
			count = 0;

		return text;
	}

	private Object getRandomText() {
		Object text = null;

		if (randomQueue.isEmpty())
			generateRandomNumbers();
		text = jedis.lindex(Parameters.REDIS_KGS, randomQueue.poll());

		return text;
	}

	private void generateRandomNumbers() {
		Set<Integer> set = new HashSet<>();
		int num;

		while (set.size() < Parameters.KEY_NUMBER) {
			num = (int) (Math.random() * Parameters.KEY_NUMBER);
			set.add((int) (Math.random() * Parameters.KEY_NUMBER));
			randomQueue.offer(num);
		}
	}

	private Jedis getConnectedJedis() {
		if (jedis != null)
			return jedis;

		try {
			jedis = new Jedis(parameters.HOST, Parameters.REDIS_PORT);
			len = jedis.llen(Parameters.REDIS_KGS);
		} catch (Exception e) {
			e.printStackTrace();
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
			e.printStackTrace();
		}

		jedis = null;
	}
}
