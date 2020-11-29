package insa.sdbd.community.reversion;
import org.apache.giraph.master.DefaultMasterCompute;

public class GraphReversionMasterComputation  extends DefaultMasterCompute {


	@Override
	public void compute() {
		long superStep = getSuperstep();
		if(superStep == 0){
			setComputation(DeleteEdgesWithMessageComputation.class);
		}
		else{
			setComputation(AddReceivedEdgesComputation.class);
		}
	}
}
