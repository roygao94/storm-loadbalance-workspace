package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import io.KGS;
import io.Parameters;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * Created by Roy Gao on 1/9/2016.
 */
public class DBolt implements IRichBolt {

	TopologyContext context;
	OutputCollector _collector;
	int myNumber;

	private String host;
	private int port;
	private transient Jedis jedis;

	private Map<Integer, KGS> infoList = new HashMap<>();
	private int load;

	public DBolt(String host, int port) {
		this.host = host;
		this.port = port;
	}

	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.context = context;
		_collector = collector;
		myNumber = context.getThisTaskIndex();
		load = 0;
	}

	@Override
	public void execute(Tuple tuple) {
		Jedis jedis = getConnectedJedis();

		if (jedis.exists(Parameters.REDIS_LOAD + myNumber)) {
			// emit sum to Controller
			_collector.emitDirect(context.getComponentTasks("controller").get(0),
					new Values(Parameters.REDIS_LOAD_REPORT, myNumber, load, ""));
			load = 0;
			jedis.del(Parameters.REDIS_LOAD + myNumber);

		} else if (jedis.exists(Parameters.REDIS_DETAIL + myNumber)) {
			// emit detail to Controller
			String detailInfo = getDetailInfo();
			_collector.emitDirect(context.getComponentTasks("controller").get(0),
					new Values(Parameters.REDIS_DETAIL_REPORT, myNumber, load, detailInfo));
			// infoList.clear();
			jedis.del(Parameters.REDIS_DETAIL + myNumber);

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

	private String getDetailInfo() {
		String detailInfo = "";
		List<Map.Entry<Integer, KGS>> tempList = new ArrayList<>(infoList.entrySet());

		Collections.sort(tempList, new Comparator<Map.Entry<Integer, KGS>>() {
			@Override
			public int compare(Map.Entry<Integer, KGS> o1, Map.Entry<Integer, KGS> o2) {
				return o1.getValue().compareTo(o2.getValue());
			}
		});

		for (Map.Entry<Integer, KGS> entry : tempList)
			detailInfo += entry.getValue().getKey() + ","
					+ entry.getValue().getG() + ","
					+ entry.getValue().getS() + "\t";

		return detailInfo;
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
		declarer.declare(new Fields("report-head", "id", "load", "detail"));
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
			e.printStackTrace();
		}

		return jedis;
	}
}
