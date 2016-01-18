package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import balancing.Balancer;
import balancing.io.NodeWithCursor;
import conf.Parameters;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Roy Gao on 1/13/2016.
 */
public class Controller implements IRichBolt {

	TopologyContext context;
	OutputCollector _collector;
	int DBoltNumber;

	private Parameters parameters;
//	private boolean balance;
//	private String host;
//	private String head;
//	private int port = Parameters.REDIS_PORT;
	private transient Jedis jedis;

	private Map<Integer, Integer> loadList;
	private Map<Integer, NodeWithCursor> detailList;
	private int loadReportRound;
	private int detailReportRound;

	public Controller(Parameters parameters) {
		this.parameters = new Parameters(parameters);
	}

	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.context = context;
		_collector = collector;
		DBoltNumber = context.getComponentTasks(Parameters.DBOLT_NAME).size();

		loadList = new HashMap<>();
		detailList = new HashMap<>();
		loadReportRound = detailReportRound = 0;
	}

	@Override
	public void execute(Tuple tuple) {
		Jedis jedis = getConnectedJedis();
		String reportHead = tuple.getValue(0).toString();

		if (reportHead.equals(Parameters.REDIS_LOAD_REPORT + "-" + loadReportRound)) {
			// receive summary report from DBolt
			int boltNumber = (int) tuple.getValue(1);
			int boltLoad = (int) tuple.getValue(2);
			loadList.put(boltNumber, boltLoad);

			if (loadList.size() == DBoltNumber) {
				int sum = 0;
				for (Map.Entry<Integer, Integer> entry : loadList.entrySet())
					sum += entry.getValue();

				int average = sum / DBoltNumber;
				boolean balanced = true;

				for (Map.Entry<Integer, Integer> entry : loadList.entrySet())
					if (entry.getValue() > average * (1 + Parameters.BALANCED_INDEX)) {
						balanced = false;

						for (int i = 0; i < DBoltNumber; ++i) {
							jedis.lpush(parameters.REDIS_HEAD + Parameters.REDIS_DETAIL + i, "");
//							jedis.lpush(head + "imbalanced-" + loadReportRound, i + "-" + loadList.get(i));
						}
						break;
					}

//				if (balanced)
//					for (int i = 0; i < DBoltNumber; ++i)
//						jedis.lpush(head + "balanced-" + loadReportRound, i + "-" + loadList.get(i));

				loadList.clear();
				loadReportRound++;
			}

		} else if (reportHead.equals(Parameters.REDIS_DETAIL_REPORT + "-" + detailReportRound)) {
			// receive detail report from DBolt
			int boltNumber = (int) tuple.getValue(1);
			String detailInfo = tuple.getValue(3).toString();

			NodeWithCursor node = new NodeWithCursor(boltNumber, detailInfo);
			detailList.put(boltNumber, node);

			if (detailList.size() == DBoltNumber) {
//				jedis.lpush(head + Parameters.REDIS_DETAIL_REPORT + "-" + detailReportRound + "-all-received", "");

				if (parameters.BALANCE) {
					Map<Integer, Integer> newRouting = Balancer.reBalance(detailList);
					if (newRouting.size() > 0) {
						String routingInfo = "";
						for (Map.Entry<Integer, Integer> entry : newRouting.entrySet())
							routingInfo += entry.getKey() + ":" + entry.getValue() + "\t";

						int UBoltNumber = context.getComponentTasks(Parameters.UBOLT_NAME).size();
						for (int i = 0; i < UBoltNumber; ++i)
							jedis.set(parameters.REDIS_HEAD + Parameters.REDIS_RT + i, routingInfo);
					}
				}
				// send massage to update routing table and adjust bolts
				jedis.lpush(parameters.REDIS_HEAD + "rebalanced-" + detailReportRound, "");

				detailList.clear();
				detailReportRound++;
			}

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
			jedis = new Jedis(parameters.HOST, Parameters.REDIS_PORT);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return jedis;
	}
}
