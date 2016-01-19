package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import balancing.Balancer;
import balancing.util.NodeWithCursor;
import conf.Parameters;
import redis.clients.jedis.Jedis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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
					if (entry.getValue() > average * (1 + parameters.getBalanceIndex())) {
						balanced = false;

						for (int i = 0; i < DBoltNumber; ++i) {
							jedis.lpush(parameters.getRedisHead() + Parameters.REDIS_DETAIL + i, "");
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

				if (parameters.getBalance()) {
					long start = System.currentTimeMillis();
					Map<Integer, Integer> newRouting = Balancer.reBalance(detailList, parameters.getBalanceIndex());
					if (newRouting.size() > 0) {
						String routingInfo = "";
						for (Map.Entry<Integer, Integer> entry : newRouting.entrySet())
							routingInfo += entry.getKey() + ":" + entry.getValue() + "\t";

						int UBoltNumber = context.getComponentTasks(Parameters.UBOLT_NAME).size();
						for (int i = 0; i < UBoltNumber; ++i)
							jedis.set(parameters.getRedisHead() + Parameters.REDIS_RT + i, routingInfo);

						long timeElapsed = System.currentTimeMillis() - start;
						jedis.lpush(parameters.getRedisHead() + "rebalanced-" + detailReportRound + "--" + timeElapsed,
								"");

						try {
							File tempDir = new File(parameters.getBaseDir() + parameters.getTopologyName());
							if (!tempDir.exists())
								tempDir.mkdirs();

							BufferedWriter writer = new BufferedWriter(new FileWriter(
									tempDir.getAbsolutePath() + "/rebalance.txt"));
							writer.write(timeElapsed + "ms" + " ," + newRouting.size());
							writer.close();

							if (parameters.isRemoteMode()) {
								Runtime runtime = Runtime.getRuntime();
								runtime.exec("scp" + " "
										+ parameters.getBaseDir() + parameters.getTopologyName() + "/rebalance.txt"
										+ " " + "admin@blade56:~/roy/temp/" + parameters.getTopologyName());
							}

						} catch (Exception e) {
						}
					}
				}
				// send massage to update routing table and adjust bolts

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
			jedis = new Jedis(parameters.getHost(), Parameters.REDIS_PORT);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return jedis;
	}
}
