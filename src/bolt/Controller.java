package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import io.Parameters;
import redis.clients.jedis.Jedis;
import util.ReportManager;

import java.util.HashMap;
import java.util.List;
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

	private Map<Integer, Integer> loadList;
	private Map<Integer, Integer> detailList;

	public Controller(String host, int port) {
		this.host = host;
		this.port = port;
	}

	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.context = context;
		_collector = collector;
		DBoltNumber = context.getComponentTasks("d-bolt").size();

		loadList = new HashMap<>();
		detailList = new HashMap<>();
	}

	@Override
	public void execute(Tuple tuple) {
		Jedis jedis = getConnectedJedis();
		String reportHead = tuple.getValue(0).toString();

		if (reportHead.equals(Parameters.REDIS_LOAD_REPORT)) {
			// receive summary report from DBolt
			int boltNumber = (int) tuple.getValue(1);
			int boltLoad = (int) tuple.getValue(2);
			loadList.put(boltNumber, boltLoad);

			if (loadList.size() == DBoltNumber) {
				int sum = 0;
				for (Map.Entry<Integer, Integer> entry : loadList.entrySet())
					sum += entry.getValue();

				int average = sum / context.getComponentTasks("d-bolt").size();
				boolean balanced = true;

				for (Map.Entry<Integer, Integer> entry : loadList.entrySet())
					if (entry.getValue() > average * Parameters.balancedIndex) {
						balanced = false;
//						for (int i = 0; i < DBoltNumber; ++i)
//							jedis.lpush(Parameters.REDIS_DETAIL + i);
						break;
					}

				if (balanced)
					System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ balanced");
				else
					System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ NOT balanced");

				loadList.clear();
			}

		} else if (reportHead.equals("detail-report")) {
			// receive detail report from DBolt

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
			e.printStackTrace();
		}

		return jedis;
	}
}
