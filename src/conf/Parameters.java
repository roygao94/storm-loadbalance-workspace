package conf;

/**
 * Created by roy on 1/12/16.
 */
public class Parameters {

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

	public String TOPOLOGY_NAME;
	public boolean BALANCE;
	public String HOST;
	public String REDIS_HEAD;

	public static long REPORT_TIME = 10000;
	public static final long LOCAL_TIME = 50000;

	public static final int KEY_NUMBER = 10000;
	public static int DBOLT_NUMBER = 10;

	public static int WINDOW_SIZE = 5;
	public static int POW_EXP = 3;

	public static double BALANCED_INDEX = 1.15;

	public Parameters() {
		TOPOLOGY_NAME = DEFAULT_TOPOLOGY_NAME;
		BALANCE = false;
		HOST = REMOTE_HOST;
		REDIS_HEAD = "TOPO-";
	}
}
