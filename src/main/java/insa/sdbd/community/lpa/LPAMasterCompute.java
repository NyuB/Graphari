package insa.sdbd.community.lpa;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;

public class LPAMasterCompute extends DefaultMasterCompute {

	public static String SHIFTED_AGG = "insa.sdbd.community.lpa.SHIFTED_AGG";
	public static IntConfOption MAX_STEPS = new IntConfOption("insa.MAX_STEPS",150, "Maximum changes of community");

	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {
		registerAggregator(SHIFTED_AGG, BooleanOrAggregator.class);
	}

	@Override
	public void compute() {
		if(getSuperstep()==0){
			setComputation(LPAComputation.class);
		}
		else if (getSuperstep()>=MAX_STEPS.get(getConf())){
			haltComputation();
		}
		else {
			BooleanWritable shifted = getAggregatedValue(SHIFTED_AGG);
			if(!shifted.get()) {
				haltComputation();
			}
		}


	}
}
