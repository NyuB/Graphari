package insa.sdbd.community.formats.vof;

import insa.sdbd.community.LabelInOut;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class VertexWithTriplongValueTextOutput extends TextVertexOutputFormat<LongWritable, LabelInOut, DoubleWritable> {
	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		//TODO
		return new VertexWithTriplongValueWriter();
	}

	private class VertexWithTriplongValueWriter extends TextVertexWriter{
		@Override
		public void writeVertex(Vertex<LongWritable, LabelInOut, DoubleWritable> vertex) throws IOException, InterruptedException {
			StringBuilder res = new StringBuilder();
			res.append(vertex.getId().get());
			res.append(" ");
			res.append(vertex.getValue().get(0).get() + " ");
			res.append(vertex.getValue().get(1).get() + " ");
			res.append(vertex.getValue().get(2).get());
			getRecordWriter().write(new Text(res.toString()), null);

		}
	}
}
