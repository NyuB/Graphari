package insa.sdbd.community;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.HashSet;

public class RevertedGraphComputation extends BasicComputation<LongWritable, LongWritable, DoubleWritable, NullWritable> {

	private Edge<LongWritable,DoubleWritable> createEdge(LongWritable destination, DoubleWritable weight) {
		return new Edge<LongWritable, DoubleWritable>() {
			@Override
			public LongWritable getTargetVertexId() {
				return destination;
			}

			@Override
			public DoubleWritable getValue() {
				//TODO
				return weight;
			}
		};
	}
	@Override
	public void compute(Vertex<LongWritable, LongWritable, DoubleWritable> vertex, Iterable<NullWritable> iterable) throws IOException {
		if(getSuperstep() == 0){
			HashSet<Long> ids = new HashSet<>();
			for(Edge<LongWritable, DoubleWritable> edge : vertex.getMutableEdges()) {
				addEdgeRequest(edge.getTargetVertexId(), createEdge(vertex.getId(), edge.getValue()));
				ids.add(edge.getTargetVertexId().get());
			}
			for(Long id : ids){
				removeEdgesRequest(vertex.getId(),new LongWritable(id));
			}
		}
		vertex.voteToHalt();

	}
}
