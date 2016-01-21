package balancing;

import balancing.util.Pair;

import java.util.Map;

/**
 * Created by roy on 1/21/16.
 */
public class BalanceInfo {

	private long time;
	private long cost;
	private Map<Integer, Integer> routingTable;
	private Map<Pair<Integer, Integer>, Integer> migrationPlan;

	public BalanceInfo() {
	}

	public void setTime(long time) {
		this.time = time;
	}

	public void setCost(long cost) {
		this.cost = cost;
	}

	public void setRoutingTable(Map<Integer, Integer> routingTable) {
		this.routingTable = routingTable;
	}

	public void setMigrationPlan(Map<Pair<Integer, Integer>, Integer> migrationPlan) {
		this.migrationPlan = migrationPlan;
	}

	public long getTime() {
		return time;
	}

	public int getRoutingSize() {
		return routingTable.size();
	}

	public long getCost() {
		return cost;
	}

	public Map<Integer, Integer> getRoutingTable() {
		return routingTable;
	}

	public Map<Pair<Integer, Integer>, Integer> getMigrationPlan() {
		return migrationPlan;
	}
}
