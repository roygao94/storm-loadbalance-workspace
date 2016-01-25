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

	int DBoltNumber;

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
		DBoltNumber = context.getComponentTasks(Parameters.DBOLT_NAME).size();
	}

	@Override
	public void nextTuple() {
		Jedis jedis = getConnectedJedis();
		if (jedis == null)
			return;

		if (lastWrite == 0 || System.currentTimeMillis() - lastWrite > Parameters.REPORT_TIME * 10) {
			try {
				List<String> keys = jedis.lrange(Parameters.REDIS_KGS, 0, len);

				File tempDir = new File(parameters.getBaseDir() + parameters.getTopologyName());
				if (!tempDir.exists())
					tempDir.mkdirs();

				BufferedWriter writer = new BufferedWriter(new FileWriter(tempDir.getAbsolutePath() + "/keys.txt"));
				int first = Integer.parseInt(keys.get(0).split(",")[1]);

				for (int i = 0; i <= 10; ++i)
					writer.write(keys.get(i) + "\n");
				for (int i = 10; i < 100; i += 30)
					writer.write(keys.get(i) + "\n");
				for (int i = 100; i < first; i += 10)
					writer.write(keys.get(i) + "\n");
				writer.close();

				if (parameters.isRemoteMode()) {
					Runtime runtime = Runtime.getRuntime();
					runtime.exec("scp" + " "
							+ parameters.getBaseDir() + parameters.getTopologyName() + "/keys.txt"
							+ " " + "admin@blade56:~/apache-storm-0.10.0/public/roy/" + parameters.getTopologyName());
				}

			} catch (Exception e) {
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
		if (++count >= len) {
			for (int i = 0; i < DBoltNumber; ++i)
				jedis.lpush(parameters.getRedisHead() + Parameters.REDIS_LOAD + i, "");

			count = 0;
		}

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
