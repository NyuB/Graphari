package insa.sdbd.community;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.HashSet;

public class DeleteEdgesComputation extends BasicComputation<LongWritable, LongWritable, DoubleWritable, NullWritable> {

	@Override
	public void compute(Vertex<LongWritable, LongWritable, DoubleWritable> vertex, Iterable<NullWritable> iterable) throws IOException {
		if(getSuperstep() == 0){
			HashSet<Long> ids = new HashSet<>();
			for(Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
				ids.add(edge.getTargetVertexId().get());
			}
			for(Long id : ids){
				vertex.removeEdges(new LongWritable(id));
			}
		}
		else{
			vertex.voteToHalt();
		}

	}
}
