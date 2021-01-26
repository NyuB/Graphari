package insa.sdbd.community.lpa;

import insa.sdbd.community.LabelInOut;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class LPAComputation extends BasicComputation<LongWritable, LongWritable, DoubleWritable, LongWritable> {


	static LongConfOption MAX_SHIFTS = new LongConfOption("insa.MAX_SHIFTS", 42L, "Maximum changes of community");
	private static Logger logger = Logger.getLogger(LPAComputation.class);
	private Random random = new Random();
	
	private Long selectNextLabel(Vertex<LongWritable, LongWritable, DoubleWritable> vertex, Iterable<LongWritable> messages) {
		HashMap<Long, Long> count = new HashMap<>();
		for (LongWritable triple : messages) {
			count.put(triple.get(), 1 + count.getOrDefault(triple.get(), 0L));
		}
		//Count vertex label
		count.put(vertex.getValue().get(), 1 + count.getOrDefault(vertex.getValue().get(), 0L));
		Long max = 1L;
		List<Long> candidates = new LinkedList<>();
		for (Map.Entry<Long, Long> keyVal : count.entrySet()) {
			if (keyVal.getValue() > max) {
				max = keyVal.getValue();
				candidates.clear();
				candidates.add(keyVal.getKey());
			} else if (keyVal.getValue().equals(max)) {
				candidates.add(keyVal.getKey());
			}
		}

		return candidates.get(random.nextInt(candidates.size()));
	}

	@Override
	public void compute(Vertex<LongWritable, LongWritable, DoubleWritable> vertex, Iterable<LongWritable> iterable) throws IOException {
		if (getSuperstep() == 0) {
			vertex.setValue((vertex.getId()));
			aggregate(LPAMasterCompute.SHIFTED_AGG, new BooleanWritable(true));
			LongWritable ping = vertex.getId();
			for (Edge<LongWritable, DoubleWritable> e : vertex.getEdges()) {
				sendMessage(e.getTargetVertexId(), ping);
			}
		} else {
			Long next = selectNextLabel(vertex, iterable);
			if (next != vertex.getValue().get()) {
				vertex.setValue(new LongWritable(next));
				aggregate(LPAMasterCompute.SHIFTED_AGG, new BooleanWritable(true));
			}
			for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
				sendMessage(edge.getTargetVertexId(), vertex.getValue());
			}
		}

	}
}