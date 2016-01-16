package test;

import balancing.Balancer;
import balancing.io.NodeWithCursor;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by roy on 1/16/16.
 */
public class NodeTest {

	public static void main(String[] args) {
		NodeWithCursor node = new NodeWithCursor(0, "1,1,1\t2,2,2\t3,3,3\t4,4,4\t5,5,5");
		Map<Integer, NodeWithCursor> nodeList = new HashMap<>();
		for (int i = 0; i < 10; ++i)
			nodeList.put(i, new NodeWithCursor(node));
		Balancer.reBalance(nodeList);

		System.out.println(node.infoList.size());
	}
}
