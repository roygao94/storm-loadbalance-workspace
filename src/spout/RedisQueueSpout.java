package spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import conf.Parameters;
import redis.clients.jedis.Jedis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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
	private long lastWrite = 0;
	private transient Jedis jedis = null;

	private Queue<Integer> randomQueue;

	public RedisQueueSpout(Parameters parameters) {
		this.parameters = new Parameters(parameters);
		randomQueue = new LinkedList<>();
		lastWrite = 0;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(OUTPUT_FIELD));
	}

	@Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void nextTuple() {
		Jedis jedis = getConnectedJedis();
		if (jedis == null)
			return;

		if (lastWrite == 0 || lastWrite - System.currentTimeMillis() > Parameters.REPORT_TIME) {
			long start = System.currentTimeMillis();
			try {
				List<String> keys = jedis.lrange(Parameters.REDIS_KGS, 0, len);

				File tempDir = new File("/home/admin/roy/temp/" + parameters.getTopologyName());
				if (!tempDir.exists())
					tempDir.mkdirs();

				BufferedWriter writer = new BufferedWriter(new FileWriter(tempDir.getAbsolutePath() + "/keys.txt"));
				for (String key : keys)
					writer.write(key);
				writer.close();

				Runtime runtime = Runtime.getRuntime();
				runtime.exec("scp /home/admin/roy/temp/" + parameters.getTopologyName()
						+ "/keys.txt admin@blade56:~/roy/temp/" + parameters.getTopologyName());

			} catch (Exception e) {
				jedis.set("fail", "");
			}

			lastWrite = System.currentTimeMillis();

		}

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
			jedis = new Jedis(parameters.getHost(), Parameters.REDIS_PORT);
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
