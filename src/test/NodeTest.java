package test;

import balancing.Balancer;
import balancing.io.KGS;
import balancing.io.NodeWithCursor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by roy on 1/16/16.
 */
public class NodeTest {

	public static void main(String[] args) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader("equal-10000.txt"));
		NodeWithCursor[] node = new NodeWithCursor[10];
		for (int i = 0; i < 10; ++i)
			node[i] = new NodeWithCursor();

		String line;
		for (int count = 0; (line = reader.readLine()) != null; ) {
			count++;
			int key = count;
			int g = Integer.parseInt(line);
			node[key % 10].add(new KGS(key, g, 1));
		}

		Map<Integer, NodeWithCursor> nodeList = new HashMap<>();
		for (int i = 0; i < 10; ++i)
			nodeList.put(i, node[i]);

		Balancer.reBalance(nodeList, 0.1);
	}
}
