package insa.sdbd.community.formats.eif;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class TextLongDirectedDouble extends TextEdgeInputFormat<LongWritable, DoubleWritable> {
	@Override
	public EdgeReader<LongWritable, DoubleWritable> createEdgeReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
		//TODO
		return null;
	}

	private class ThreeStrings {
		String left;
		String right;
		String weight = "1.0";

		public ThreeStrings(String left, String right) {
			this.left = left;
			this.right = right;
		}

		public ThreeStrings(String left, String right, String weight) {
			this.left = left;
			this.right = right;
			this.weight = weight;
		}
	}

	class CustomReader extends TextEdgeReaderFromEachLineProcessed<ThreeStrings> {
		@Override
		protected ThreeStrings preprocessLine(Text text) throws IOException {
			String[] line = text.toString().split(" ");
			if(line.length<2 || line.length>3){
				throw new IOException("Edge format requires between 2 and 3 values");
			}
			else if (line.length == 2){
				return new ThreeStrings(line[0],line[1]);
			}
			else {
				return new ThreeStrings(line[0], line[1], line[2]);
			}
		}
		@Override
		protected LongWritable getTargetVertexId(ThreeStrings threeStrings) throws IOException {
			return new LongWritable(Long.parseLong(threeStrings.right));
		}

		@Override
		protected LongWritable getSourceVertexId(ThreeStrings threeStrings) throws IOException {
			return new LongWritable(Long.parseLong(threeStrings.left));
		}

		@Override
		protected DoubleWritable getValue(ThreeStrings threeStrings) throws IOException {
			return new DoubleWritable(Double.parseDouble(threeStrings.weight));
		}
	}
}
