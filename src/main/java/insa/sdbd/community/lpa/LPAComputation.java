package insa.sdbd.community.lpa;

import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;


import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LPAComputation extends BasicComputation<LongWritable, LongWritable, DoubleWritable, LongWritable> {

	
	public static LongConfOption MAX_SHIFTS = new LongConfOption("insa.MAX_SHIFTS",42L, "Maximum changes of community");


	private long shifts;
	private static Logger logger = Logger.getLogger(LPAComputation.class);

	private Long selectNextLabel(Vertex<LongWritable, LongWritable, DoubleWritable> vertex,Iterable<LongWritable> messages){
		HashMap<Long,Long> count =new HashMap<>();
		for(LongWritable label : messages){
			count.put(label.get(),1+count.getOrDefault(label.get(),0L));
		}
		Long max = -1L;
		Long label = vertex.getValue().get();
		for(Map.Entry<Long,Long> keyVal : count.entrySet()){
			if (keyVal.getValue() > max){
				max= keyVal.getValue();
				label = keyVal.getKey();
			}
			else if(keyVal.getValue().equals(max)){
				label = Long.min(label,keyVal.getKey());
			}
		}
		if(max == 1L){
			label = Long.min(label,vertex.getValue().get());
		}
		return label;
	}

	@Override
	public void compute(Vertex<LongWritable, LongWritable, DoubleWritable> vertex, Iterable<LongWritable> iterable) throws IOException {
		if(getSuperstep() == 0){
			vertex.setValue(vertex.getId());
			aggregate(LPAMasterCompute.SHIFTED_AGG,new BooleanWritable(true));
		}
		else{
			Long next = selectNextLabel(vertex,iterable);
			if(next != vertex.getValue().get()){
				vertex.setValue(new LongWritable(next));
				aggregate(LPAMasterCompute.SHIFTED_AGG,new BooleanWritable(true));
			}
		}
		for(Edge<LongWritable,DoubleWritable> edge : vertex.getEdges()){
			sendMessage(edge.getTargetVertexId(), vertex.getValue());
		}
	}
}