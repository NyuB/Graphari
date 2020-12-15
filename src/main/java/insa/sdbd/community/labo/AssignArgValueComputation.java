package insa.sdbd.community.labo;

import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class AssignArgValueComputation extends BasicComputation<LongWritable, LongWritable, DoubleWritable, NullWritable> {
	//use -ca
	public static LongConfOption ASSIGNED = new LongConfOption("insa.ASSIGNED",42L,"Default value assigned to each vertex");

	@Override
	public void compute(Vertex<LongWritable, LongWritable, DoubleWritable> vertex, Iterable<NullWritable> iterable) throws IOException {
		if(getSuperstep()==0){
			vertex.setValue(new LongWritable(ASSIGNED.get(getConf())));
		}
		vertex.voteToHalt();
	}
}
