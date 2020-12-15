package insa.sdbd.community.formats.vof;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class VertexWithLongValueDoubleEdgeTextOutput extends TextVertexOutputFormat<LongWritable, LongWritable, DoubleWritable> {
	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		//TODO
		return new VertexWithLongValueDoubleEdgesWriter();
	}

	class VertexWithLongValueDoubleEdgesWriter extends TextVertexWriter{
		@Override
		public void writeVertex(Vertex<LongWritable, LongWritable, DoubleWritable> vertex) throws IOException, InterruptedException {
			StringBuilder res = new StringBuilder();
			res.append("[");
			res.append(vertex.getId().get());
			res.append(", ");
			res.append(vertex.getValue().get());
			res.append(", [");
			boolean start = true;
			for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
				if(!start){
					res.append(", ");
				}
				start = false;
				res.append("[");
				res.append(edge.getTargetVertexId().get());
				res.append(", ");
				res.append(edge.getValue().get());
				res.append("]");
			}
			res.append("]]");
			getRecordWriter().write(new Text(res.toString()), null);

		}
	}
}
