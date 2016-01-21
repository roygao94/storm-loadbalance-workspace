package balancing;

import balancing.util.*;
import conf.Parameters;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * Created by Roy Gao on 1/15/2016.
 */
public class Balancer {

	private static NodeWithCursor[] node;
	private static NodeWithCursor[] copyOfNode;
	private static historyS[] history;
	private static int N;
	private static int upperBound;
	private static int lowerBound;
	private static PriorityQueue<KGS> publicSet = new PriorityQueue<>(Parameters.KEY_NUMBER, new Comparator<KGS>() {
		@Override
		public int compare(KGS o1, KGS o2) {
			return -((Integer) o1.getG()).compareTo(o2.getG());
		}
	});
	public static Map<Integer, Integer> routing;
	public static Map<Pair<Integer, Integer>, Integer> migrationPlan;

	private static transient Jedis jedis;

	public static BalanceInfo reBalance(Map<Integer, NodeWithCursor> nodeList, double balanceIndex) {
		long start = System.currentTimeMillis();

		if (history == null) {
			history = new historyS[Parameters.KEY_NUMBER + 1];
			for (int i = 0; i < Parameters.KEY_NUMBER + 1; ++i)
				history[i] = new historyS(Parameters.WINDOW_SIZE);
		}

		N = nodeList.size();
		node = new NodeWithCursor[N];
		for (int i = 0; i < N; ++i)
			node[i] = nodeList.get(i);

		computeBound(balanceIndex);
		backup();

		migrateBack();

		migrate();

		updateRouting();

		long timeElapsed = System.currentTimeMillis() - start;

		BalanceInfo info = new BalanceInfo();
		info.setTime(timeElapsed);
		info.setCost(getCostAndMigrationPlan());
		info.setRoutingTable(routing);
		info.setMigrationPlan(migrationPlan);

		return info;

		// push new routing into redis
	}

	private static void computeBound(double balanceIndex) {
		int total = 0;
		for (int i = 0; i < N; ++i)
			total += node[i].getTotalLoad();
		int average = total / N;

		upperBound = (int) (average * (1 + balanceIndex));
		lowerBound = (int) (average * (1 - balanceIndex));
		System.out.println(average + "\t" + upperBound);
	}

	private static void backup() {
		copyOfNode = new NodeWithCursor[N];
		for (int i = 0; i < N; ++i)
			copyOfNode[i] = new NodeWithCursor(node[i]);
	}

//	private static int getRoutingSize() {
//		int count = 0;
//
//		for (int i = 0; i < N; ++i)
//			for (KGS kgs : node[i].infoList.values())
//				if (kgs.getKey() % N != i)
//					count++;
//
//		return count;
//	}

	private static void updateRouting() {
		if (routing != null)
			routing.clear();
		else
			routing = new HashMap<>();

		for (int i = 0; i < N; ++i)
			for (KGS kgs : node[i].infoList.values())
				if (kgs.getKey() % N != i)
					routing.put(kgs.getKey(), i);
	}

	private static void migrateBack() {
		List<MigrationKGS> backList = new ArrayList<>();
		for (int i = 0; i < N; ++i) {
			for (KGS kgs : node[i].infoList.values()) {
				if (kgs.getKey() % N != i) {
					backList.add(new MigrationKGS(i, kgs));
				}
			}
		}


		for (MigrationKGS migrationKGS : backList) {
			int cNid = migrationKGS.getNodeID();
			KGS kgs = migrationKGS.getInfo();
			int keyid = kgs.getKey();

//			copyOfNode[keyid % N].add(kgs);
//			copyOfNode[cNid].remove(keyid);

			node[keyid % N].add(kgs);
			node[cNid].remove(keyid);
		}
	}

	private static void migrate() {
		for (int i = 0; i < N; ++i)
			if (node[i].getTotalLoad() > upperBound) {
				Map<Integer, KGS> moveList = getMigrationOutGroup(i);
				publicSet.addAll(moveList.values());
			}

		putPublicSetToLowNodes();
	}

	private static Map<Integer, KGS> getMigrationOutGroup(int i) {
		int cursor = node[i].getCursor();
		List<KGS> thisNode = new ArrayList<>(node[i].infoList.values());
		Collections.sort(thisNode, new Comparator<KGS>() {
			@Override
			public int compare(KGS o1, KGS o2) {
				int s1 = o1.getS();
				int s2 = o2.getS();
				if (Parameters.WINDOW_SIZE > 1) {
					s1 += history[o1.getKey()].getHistorySum();
					s2 += history[o2.getKey()].getHistorySum();
				}

				double g1 = powerG(o1.getG());
				double g2 = powerG(o2.getG());

				if (((Double) (s1 / g1)).compareTo(s2 / g2) == 0) return -((Integer) o1.getG()).compareTo(o2.getG());
				else return (((Double) (s1 / g1)).compareTo(s2 / g2));
			}

			private double powerG(int g) {
				return Math.pow(g, Parameters.POW_EXP);
			}
		});

		Queue<KGS> publicSet2 = new PriorityQueue<>(Parameters.KEY_NUMBER, new Comparator<KGS>() {
			@Override
			public int compare(KGS o1, KGS o2) {
				return -((Integer) o1.getG()).compareTo(o2.getG());
			}
		});

		int minGoal = node[i].getTotalLoad() - upperBound;
		int sumG = 0;

		if (Parameters.ENSURE_LOW) {
			int maxGoal = node[i].getTotalLoad() - lowerBound;
			boolean flag = true;
			int mark = 0;

			for (int j = 0; sumG < minGoal && j < thisNode.size(); ++j)
				if (thisNode.get(j).getG() < cursor && sumG + thisNode.get(j).getG() <= maxGoal) {
					publicSet2.add(new KGS(thisNode.get(j)));
					sumG += thisNode.get(j).getG();
				} else if (flag && thisNode.get(j).getG() < cursor && sumG + thisNode.get(j).getG() > maxGoal) {
					flag = false;
					mark = j;
				}

			if (sumG < minGoal && !flag) {
				publicSet2.add(new KGS(thisNode.get(mark)));
				sumG += thisNode.get(mark).getG();
			}

		} else
			for (int j = 0; sumG < minGoal && j < thisNode.size(); ++j)
				if (thisNode.get(j).getG() < cursor) {
					publicSet2.add(new KGS(thisNode.get(j)));
					sumG += thisNode.get(j).getG();
				}

		if (sumG >= minGoal) {
			Map<Integer, KGS> moveList = new HashMap<>();

			while (!publicSet2.isEmpty()) {
				KGS kgs = publicSet2.poll();
				int key = kgs.getKey();
				int origin = kgs.getKey() % N;
				node[i].remove(key);
				if (node[origin].getTotalLoad() + kgs.getG() < upperBound)
					node[origin].add(kgs);
				else
					moveList.put(key, kgs);
			}

			return moveList;
		} else {
			System.out.println("node " + i + " cannot be balanced");
			return new HashMap<>();
		}
	}

	private static void putPublicSetToLowNodes() {
		PriorityQueue<Pair<Integer, Integer>> low = new PriorityQueue<>(N, new Comparator<Pair<Integer, Integer>>() {
			@Override
			public int compare(Pair<Integer, Integer> o1, Pair<Integer, Integer> o2) {
				return o1.getSecond().compareTo(o2.getSecond());
			}
		});

		while (!publicSet.isEmpty()) {
			KGS kgs = publicSet.poll();
			low.clear();

			for (int i = 0; i < N; ++i) {
				low.add(new Pair<>(i, node[i].getTotalLoad()));
			}

			boolean flag = false;

			while (!low.isEmpty()) {
				int ID = low.poll().getFirst();
				if (kgs.getG() + node[ID].getTotalLoad() <= upperBound) {
					node[ID].add(kgs);
					flag = true;
					break;
				} else {
					node[ID].add(kgs);
					node[ID].setCursor(kgs.getG());
					Map<Integer, KGS> moveList = getMigrationOutGroup(ID);

					if (moveList.isEmpty())
						node[ID].remove(kgs);
					else {
						for (KGS itr : moveList.values())
							publicSet.add(itr);
						flag = true;
						break;
					}
				}
			}

			if (!flag) {
				System.out.print(kgs.toString() + "\t");
				System.out.print(kgs.toString() + "\t");
				System.out.println("failed");

				for (int i = 0; i < N; ++i)
					System.out.print(node[i].getTotalLoad() + "\t");
				System.out.println();
			}
		}
	}

	private static int getCostAndMigrationPlan() {
		if (migrationPlan != null)
			migrationPlan.clear();
		else
			migrationPlan = new HashMap<>();
		int cost = 0;

		for (int i = 0; i < N; ++i)
			for (KGS kgs : copyOfNode[i].infoList.values()) {
				if (!node[i].infoList.containsKey(kgs.getKey())) {
					for (int j = 0; j < N; ++j)
						if (node[j].infoList.containsKey(kgs.getKey())) {
							Pair<Integer, Integer> fromTo = new Pair<>(i, j);
							if (migrationPlan.containsKey(fromTo))
								migrationPlan.put(fromTo, migrationPlan.get(fromTo) + kgs.getG());
							else
								migrationPlan.put(fromTo, kgs.getG());
							break;
						}

					cost += kgs.getS();
					if (Parameters.WINDOW_SIZE != 1)
						cost += history[kgs.getKey()].getHistorySum();
				}
				history[kgs.getKey()].add(kgs.getS());
			}

		return cost;
	}

	private static Jedis getConnectedJedis() {
		if (jedis != null)
			return jedis;

		try {
			jedis = new Jedis(Parameters.REMOTE_HOST, Parameters.REDIS_PORT);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return jedis;
	}
}
