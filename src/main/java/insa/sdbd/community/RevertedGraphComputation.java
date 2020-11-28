package insa.sdbd.community;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.writable.tuple.LongDoubleWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;

public class RevertedGraphComputation extends BasicComputation<LongWritable, LongWritable, DoubleWritable, LongDoubleWritable> {

	private static Logger LOG = Logger.getLogger(RevertedGraphComputation.class);

	@Override
	public void compute(Vertex<LongWritable, LongWritable, DoubleWritable> vertex, Iterable<LongDoubleWritable> iterable) throws IOException {
		if(getSuperstep() == 0){
			HashSet<Long> ids = new HashSet<>();

			for(Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
				sendMessage(edge.getTargetVertexId(), new LongDoubleWritable(vertex.getId().get(),edge.getValue().get()));
				ids.add(edge.getTargetVertexId().get());
			}
			for(Long id : ids){
				vertex.removeEdges(new LongWritable(id));
			}
		}
		else{
			for(LongDoubleWritable msg : iterable){
				Edge<LongWritable, DoubleWritable> newEdge = EdgeFactory.create(msg.getLeft(),msg.getRight());
				vertex.addEdge(newEdge);
			}

		}
		vertex.voteToHalt();

	}
}
