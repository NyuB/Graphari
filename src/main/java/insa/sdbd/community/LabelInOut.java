package insa.sdbd.community;

import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.LongWritable;

public class LabelInOut extends ArrayListWritable<LongWritable> {
	public LabelInOut() {
		super();
	}

	@Override
	public void setClass() {
		setClass(LongWritable.class);

	}
	public void setInDegree(int degree){
		this.set(1, new LongWritable(degree));
	}
	public void setOutDegree(int degree){
		this.set(2,new LongWritable(degree));
	}
	public void setLabel(LongWritable label){
		this.set(0,label);
	}
	public void setLabel(long label){
		this.setLabel(new LongWritable(label));
	}
	public long getLabel(){
		return this.get(0).get();
	}
	public long getInDegree(){
		return this.get(1).get();
	}
	public long getOutDegree(){
		return this.get(2).get();
	}
	public long getTotalDegree(){
		return this.getInDegree() + this.getOutDegree();
	}

}
