package insa.sdbd.community.formats.vif;


import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;
import java.util.List;

public class JsonLongLongDirectedDouble extends TextVertexInputFormat<LongWritable, LongWritable, DoubleWritable> {
	@Override
	public TextVertexReader createVertexReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
		//TODO
		return new JsonLongLongDoubleVertexReader();
	}

	private class JsonLongLongDoubleVertexReader extends TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray,
			JSONException> {
		@Override
		protected JSONArray preprocessLine(Text text) throws JSONException, IOException {
			return new JSONArray(text.toString());
		}

		@Override
		protected LongWritable getId(JSONArray jsonArray) throws JSONException, IOException {
			//TODO
			return new LongWritable(jsonArray.getLong(0));
		}

		@Override
		protected LongWritable getValue(JSONArray jsonArray) throws JSONException, IOException {
			return new LongWritable(jsonArray.getLong(1));
		}

		@Override
		protected Iterable<Edge<LongWritable, DoubleWritable>> getEdges(JSONArray jsonArray) throws JSONException, IOException {
			JSONArray jsonEdgeArray = jsonArray.getJSONArray(2);
			List<Edge<LongWritable, DoubleWritable>> edges =
					Lists.newArrayListWithCapacity(jsonEdgeArray.length());
			for (int i = 0; i < jsonEdgeArray.length(); ++i) {
				JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
				edges.add(EdgeFactory.create(new LongWritable(jsonEdge.getLong(0)),
						new DoubleWritable( jsonEdge.getDouble(1))));
			}
			return edges;
		}
	}
}
