package test;

import backtype.storm.tuple.Values;

/**
 * Created by roy on 1/15/16.
 */
public class ValuesTest {

	public static void main(String[] args) {
		Values val = new Values("666", 3, 31, false);

		String a = val.get(0).toString();
		int b = (int) val.get(1);
		int c = (int) val.get(2);
		boolean d = (boolean) val.get(3);

		System.out.println(a + "\n" + b + "\n" + c + "\n" + d);
	}
}