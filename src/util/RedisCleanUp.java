package util;

import io.Parameters;
import redis.clients.jedis.Jedis;

/**
 * Created by roy on 1/15/16.
 */
public class RedisCleanUp {

	public static void main(String[] args) {
		redisCleanUp(Parameters.REMOTE_HOST);
	}

	public static void redisCleanUp(String mode) {
		Jedis jedis = new Jedis(mode, Parameters.REDIS_PORT);

		for (int i = 0; i < 10; ++i) {
			if (jedis.exists(Parameters.REDIS_LOAD + i))
				jedis.del(Parameters.REDIS_LOAD + i);
			if (jedis.exists(Parameters.REDIS_DETAIL + i))
				jedis.del(Parameters.REDIS_DETAIL + i);
			if (jedis.exists("emit-" + Parameters.REDIS_LOAD + i))
				jedis.del("emit-" + Parameters.REDIS_LOAD + i);
		}

		jedis.disconnect();
	}
}
