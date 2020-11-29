package insa.sdbd.community.scc;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.writable.tuple.LongDoubleWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.log4j.Logger;

import java.io.IOException;

public class SCCRebuildRevertEdgesComputation extends BasicComputation<LongWritable, LongWritable, DoubleWritable, LongDoubleWritable> {

	@Override
	public void compute(Vertex<LongWritable, LongWritable, DoubleWritable> vertex, Iterable<LongDoubleWritable> iterable) throws IOException {
		for(LongDoubleWritable msg : iterable){
			Edge<LongWritable, DoubleWritable> newEdge = EdgeFactory.create(msg.getLeft(),msg.getRight());
			vertex.addEdge(newEdge);
		}
	}
}
