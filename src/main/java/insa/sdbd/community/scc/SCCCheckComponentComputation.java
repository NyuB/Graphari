package insa.sdbd.community.scc;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.giraph.writable.tuple.LongDoubleWritable;

import java.io.IOException;

import org.apache.log4j.Logger;

public class SCCCheckComponentComputation extends BasicComputation<LongWritable,LongWritable, DoubleWritable, LongDoubleWritable> {
	private static Logger logger = Logger.getLogger(SCCCheckComponentComputation.class);

	@Override
	public void compute(Vertex<LongWritable, LongWritable, DoubleWritable> vertex, Iterable<LongDoubleWritable> iterable) throws IOException {
		
		if(getSuperstep() == 0){
			vertex.setValue(new LongWritable(SCCMasterCompute.VERTEX_INIT));
		}

		if((vertex.getValue().get() == SCCMasterCompute.VERTEX_INIT) || (vertex.getValue().get() == SCCMasterCompute.VERTEX_REACHED)) {
			if(vertex.getNumEdges() == 0){
				vertex.setValue(vertex.getId());
			}
			else{
				vertex.setValue(new LongWritable(SCCMasterCompute.VERTEX_INIT));
				aggregate(SCCMasterCompute.VERTEX_UPDATED_AGG, new BooleanWritable(true));
				aggregate(SCCMasterCompute.CURRENT_VERTEX_AGG, vertex.getId());
			}
		}
		else {
			vertex.voteToHalt();
		}
	}
}
