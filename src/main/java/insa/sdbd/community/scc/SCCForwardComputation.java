package insa.sdbd.community;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

//INCOMPLETE
public class StronglyConnectedComponentsComputation extends BasicComputation<LongWritable, LongWritable, DoubleWritable, LongWritable> {

	private static LongWritable default_origin = new LongWritable(0);
	private static LongWritable INIT = new LongWritable(-1);
	private static LongWritable FORWARD = new LongWritable(0);
	private static LongWritable BACKWARD = new LongWritable(1);

	private boolean isForwardMessage(LongWritable message){
		return message.equals(FORWARD);
	}

	private boolean isBackWardMessage(LongWritable message){
		return message.equals(BACKWARD);
	}

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

	public void compute(Vertex<LongWritable, LongWritable, DoubleWritable> vertex, Iterable<LongWritable> iterable) throws IOException {
		if(getSuperstep() == 0){
			vertex.setValue(INIT);
			if(vertex.getId().equals(default_origin)){
				vertex.setValue(default_origin);
			}
		}

		for(LongWritable msg : iterable){
			if(isForwardMessage(msg) && vertex.getValue().equals(INIT)){
				vertex.setValue(FORWARD);
				for(Edge<LongWritable,DoubleWritable> e : vertex.getEdges()){
					sendMessage(e.getTargetVertexId(),msg);
					removeEdgesRequest(vertex.getId(),e.getTargetVertexId());
					addEdgeRequest(e.getTargetVertexId(),createEdge(vertex.getId(),e.getValue()));
				}
			}
			else if(isBackWardMessage(msg) && vertex.getValue().equals(FORWARD)){
				vertex.setValue(default_origin);
			}
		}
		vertex.voteToHalt();
	}
}
