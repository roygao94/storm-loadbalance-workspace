package test;

import java.util.*;

/**
 * Created by roy on 1/17/16.
 */
public class SetTest {

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		int num;
		Set<Integer> set;
		Queue<Integer> queue = new LinkedList<>();

		for (int i = 0; i < 100; ++i) {
			set = new HashSet<>();

			queue = new LinkedList<>();
			while (set.size() < 10000) {
				num = (int) (Math.random() * 10000);
				set.add(num);
				queue.offer(num);
			}
		}

		while (!queue.isEmpty())
			System.out.println(queue.poll());

		System.out.println(System.currentTimeMillis() - start);
	}
}
