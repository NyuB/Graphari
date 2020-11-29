package insa.sdbd.community.scc;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.writable.tuple.LongDoubleWritable;

import java.io.IOException;

public class SCCCheckComponentComputation extends BasicComputation<LongWritable,LongWritable, DoubleWritable, LongDoubleWritable> {
	@Override
	public void compute(Vertex<LongWritable, LongWritable, DoubleWritable> vertex, Iterable<LongDoubleWritable> iterable) throws IOException {
		if(getSuperstep() == 0){
			vertex.setValue(new LongWritable(SCCMasterComputation.VERTEX_INIT));
		}
		
		if((vertex.getValue().get() == SCCMasterComputation.VERTEX_INIT) || (vertex.getValue().get() == SCCMasterComputation.VERTEX_REACHED)) {
			aggregate(SCCMasterComputation.VERTEX_NOT_CLASSIFIED_AGG,new BooleanWritable(true));
			aggregate(SCCMasterComputation.VERTEX_UPDATED_AGG, new BooleanWritable(true));
			aggregate(SCCMasterComputation.CURRENT_VERTEX_AGG, vertex.getId());
		}
	}
}
