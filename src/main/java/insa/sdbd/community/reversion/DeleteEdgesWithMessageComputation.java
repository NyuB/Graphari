package insa.sdbd.community.reversion;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.writable.tuple.LongDoubleWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.HashSet;

public class DeleteEdgesWithMessageComputation extends BasicComputation<LongWritable, LongWritable, DoubleWritable, LongDoubleWritable> {

	@Override
	public void compute(Vertex<LongWritable, LongWritable, DoubleWritable> vertex, Iterable<LongDoubleWritable> iterable) throws IOException {
		HashSet<Long> ids = new HashSet<>();
		for(Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
			sendMessage(edge.getTargetVertexId(),new LongDoubleWritable(vertex.getId().get(),edge.getValue().get()));
			ids.add(edge.getTargetVertexId().get());
		}
		for(Long id : ids){
			vertex.removeEdges(new LongWritable(id));
		}
		vertex.voteToHalt();
	}
}
