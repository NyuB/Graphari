package insa.sdbd.community.scc;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.giraph.writable.tuple.LongDoubleWritable;

import org.apache.log4j.Logger;

import java.io.IOException;

public class SCCForwardComputation extends BasicComputation<LongWritable, LongWritable, DoubleWritable, LongDoubleWritable> {
	private static Logger logger = Logger.getLogger(SCCForwardComputation.class);
	
	@Override
	public void compute(Vertex<LongWritable, LongWritable, DoubleWritable> vertex, Iterable<LongDoubleWritable> iterable) throws IOException {

		if(vertex.getValue().get() == SCCMasterCompute.VERTEX_INIT){
			LongWritable rootId = getAggregatedValue(SCCMasterCompute.CURRENT_VERTEX_AGG);
			if(vertex.getId().get() == rootId.get()){
				logger.info("\n\n"+"Root vertex in forward phase for "+rootId.get()+"\n\n");
				vertex.setValue(new LongWritable(SCCMasterCompute.VERTEX_REACHED));
				aggregate(SCCMasterCompute.VERTEX_UPDATED_AGG, new BooleanWritable(true));
				for(Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
					sendMessage(edge.getTargetVertexId(), new LongDoubleWritable(0L, 0.0));
				}
			}
			else{
				boolean send = false;
				for(LongDoubleWritable msg : iterable){
					if(!send){
						send = true;
						vertex.setValue(new LongWritable(SCCMasterCompute.VERTEX_REACHED));
						aggregate(SCCMasterCompute.VERTEX_UPDATED_AGG, new BooleanWritable(true));
						for(Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
							sendMessage(edge.getTargetVertexId(), new LongDoubleWritable(0L, 0.0));
						}
					}
				}
			}
		}
	}
}
