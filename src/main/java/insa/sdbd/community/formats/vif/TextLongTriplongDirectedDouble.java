package insa.sdbd.community.formats.vif;

import insa.sdbd.community.LabelInOut;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class TextLongTriplongDirectedDouble extends TextVertexInputFormat<LongWritable, LabelInOut, DoubleWritable> {
	private class FourStringFields {
		String id;
		String value;
		List<String> edges = new LinkedList<>();

		public FourStringFields(String id, String value) {
			this.id = id;
			this.value = value;
		}
		public void addEdge(String edgeRepr){
			this.edges.add(edgeRepr);
		}
	}

	class CustomReader extends TextVertexReaderFromEachLineProcessed<FourStringFields> {
		@Override
		protected FourStringFields preprocessLine(Text text) throws IOException {
			String[] line = text.toString().split(" ");
			if(line.length <2)throw new IOException("Vertex requires at least an id and a value");
			FourStringFields res = new FourStringFields(line[0],line[1]);
			for(int i = 2;i<line.length;i++){
				res.addEdge(line[i]);
			}
			return res;
		}
		@Override
		protected Iterable<Edge<LongWritable,DoubleWritable>> getEdges(FourStringFields line) throws IOException {
			List<Edge<LongWritable,DoubleWritable>> res = new LinkedList<>();
			for(String edgeRepr : line.edges){
				Long id = null;
				double weight = 1.0;
				String[] tuple = edgeRepr.split("=");
				if(tuple.length < 1 || tuple.length>2){
					throw new IOException("Invalid edge");
				}
				else {
					id = Long.parseLong(tuple[0]);
					if (tuple.length == 2) {
						weight = Double.parseDouble(tuple[1]);
					}
					res.add(EdgeFactory.create(new LongWritable(id),new DoubleWritable(weight)));
				}
			}
			return res;
		}

		@Override
		protected LabelInOut getValue(FourStringFields line) throws IOException {
			LabelInOut res = new LabelInOut();
			res.setLabel(Long.parseLong(line.id));
			return res;
		}

		@Override
		protected LongWritable getId(FourStringFields line) throws IOException {
			return new LongWritable(Long.parseLong(line.id));
		}
	}

	@Override
	public TextVertexReader createVertexReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
		//TODO
		return new CustomReader();
	}
}
