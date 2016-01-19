package conf;

import java.io.Serializable;

/**
 * Created by roy on 1/12/16.
 */
public class Parameters implements Serializable {

	public static final String REMOTE_HOST = "10.11.1.56";
	public static final String LOCAL_HOST = "localhost";
	public static final String REDIS_VM = "192.168.56.143";
	public static final int REDIS_PORT = 6379;

	public static final String REDIS_RT = "rt";
	public static final String REDIS_KGS = "kgs";

	public static final String REDIS_LOAD = "load";
	public static final String REDIS_LOAD_REPORT = "load-report";
	public static final String REDIS_DETAIL = "detail";
	public static final String REDIS_DETAIL_REPORT = "detail-report";

	public static final String DEFAULT_TOPOLOGY_NAME = "load-balance-driver";
	public static final String SPOUT_NAME = "spout";
	public static final String UBOLT_NAME = "u-bolt";
	public static final String DBOLT_NAME = "d-bolt";
	public static final String CONTROLLER_NAME = "controller";

	public static long REPORT_TIME = 10000;

	public static final long LOCAL_TIME = 50000;
	public static final int KEY_NUMBER = 10000;

	public static int DBOLT_NUMBER = 10;
	public static int WINDOW_SIZE = 5;

	public static int POW_EXP = 3;
	public static double DEFAULT_BALANCED_INDEX = 0.1;
	public static boolean ENSURE_LOW = true;

	private String topologyName;
	private boolean balance;
	private String host;
	private String redisHead;
	private double balanceIndex;

	public Parameters() {
		topologyName = DEFAULT_TOPOLOGY_NAME;
		balance = false;
		host = REMOTE_HOST;
		redisHead = "TOPO-";
		balanceIndex = DEFAULT_BALANCED_INDEX;
	}

	public Parameters(Parameters parameters) {
		topologyName = parameters.topologyName;
		balance = parameters.balance;
		host = parameters.host;
		redisHead = parameters.redisHead;
		balanceIndex = parameters.balanceIndex;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setTopologyName(String name) {
		topologyName = name;
	}

	public void setBalance(boolean balance) {
		this.balance = balance;
	}

	public void appendRedisHead(String add) {
		redisHead += add;
	}

	public void setBalanceIndex(double balanceIndex) {
		this.balanceIndex = balanceIndex;
	}

	public String getTopologyName() {
		return topologyName;
	}

	public boolean getBalance() {
		return balance;
	}

	public String getHost() {
		return host;
	}

	public String getRedisHead() {
		return redisHead;
	}

	public double getBalanceIndex() {
		return balanceIndex;
	}
}
