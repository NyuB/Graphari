package insa.sdbd.community;

import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;


import java.io.IOException;

@Algorithm(name="Maximum Direct Accessor Id",
	description = "Return the maximum id from all neighbours which can access this node, -1 if None")
public class MaximumDirectAccessorComputation extends BasicComputation<LongWritable, LongWritable, DoubleWritable, LongWritable>{
	private static Logger LOG = Logger.getLogger(MaximumDirectAccessorComputation.class);
	private void shortRangeBroadCast(Vertex<LongWritable, LongWritable, DoubleWritable> vertex){
		for(Edge<LongWritable,DoubleWritable> edge : vertex.getEdges()){
			LOG.info("Sending message from "+vertex.getValue().get()+" to "+edge.getTargetVertexId().get());
			sendMessage(edge.getTargetVertexId(),vertex.getId());
		}
	}

	@Override
	public void compute(Vertex<LongWritable, LongWritable, DoubleWritable> vertex, Iterable<LongWritable> iterable) throws IOException {
		if(getSuperstep() == 0){
			LOG.info("Setting default value -1");
			vertex.setValue(new LongWritable(-1));
			shortRangeBroadCast(vertex);
		}
		for(LongWritable message : iterable){
			LOG.info("Vertex"+vertex.getId().get()+"with value "+vertex.getValue().get()+" receiving message "+message.get());
			vertex.setValue(new LongWritable(Long.max(vertex.getValue().get(),message.get())));
			LOG.info("Updated vertex value is "+vertex.getValue().get());
		}
		vertex.voteToHalt();

	}
}
