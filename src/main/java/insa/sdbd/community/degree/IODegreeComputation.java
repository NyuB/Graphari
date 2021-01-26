package insa.sdbd.community.degree;

import insa.sdbd.community.LabelInOut;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.join.TupleWritable;

import java.io.IOException;

public class IODegreeComputation extends BasicComputation<LongWritable, LabelInOut, DoubleWritable, LabelInOut>{
	public void compute(Vertex<LongWritable, LabelInOut, DoubleWritable> vertex, Iterable<LabelInOut> iterable) throws IOException {
		if(getSuperstep() == 0){
			vertex.setValue(new LabelInOut());
			vertex.getValue().add(vertex.getId());
			vertex.getValue().add(new LongWritable(0L));
			vertex.getValue().add(new LongWritable(vertex.getNumEdges()));
			for(Edge<LongWritable,DoubleWritable> e : vertex.getEdges()){
				sendMessage(e.getTargetVertexId(),new LabelInOut());
			}
		}
		else {
			int numMsg = 0;
			for(LabelInOut l : iterable)numMsg++;
			vertex.getValue().setInDegree(numMsg);
		}
		vertex.voteToHalt();
	}


}


