package insa.sdbd.community.scc;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.LongMinAggregator;
import org.apache.giraph.aggregators.LongOverwriteAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.log4j.Logger;

public class SCCMasterComputation extends DefaultMasterCompute {


	public static Long PHASE_FORWARD = 0L;
	public static Long PHASE_BACKWARD = 1L;
	public static Long PHASE_REVERSE_DELETE = 2L;
	public static Long PHASE_REVERSE_ADD = 3L;
	public static Long PHASE_REVERSE_DELETE_BACK = 4L;
	public static Long PHASE_REVERSE_ADD_BACK = 5L;
	public static Long PHASE_CHECK = 6L;



	public static Long VERTEX_INIT = -1L;
	public static Long VERTEX_REACHED = -2L;

	public static String PHASE_AGG = "insa.sdbd.community.scc.PHASE_AGG";
	public static String CURRENT_VERTEX_AGG = "insa.sdbd.community.scc.CURRENT_VERTEX_AGG";
	public static String VERTEX_UPDATED_AGG = "insa.sdbd.community.scc.VERTEX_UPDATED_AGG";
	private static Logger logger = Logger.getLogger(SCCMasterComputation.class);

	private Long phase = PHASE_CHECK;

	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {

		registerPersistentAggregator(PHASE_AGG, LongOverwriteAggregator.class);

		registerPersistentAggregator(CURRENT_VERTEX_AGG, LongMinAggregator.class);

		registerAggregator(VERTEX_UPDATED_AGG, BooleanOrAggregator.class);
	}

	@Override
	public void compute() {
		LongWritable current = getAggregatedValue(CURRENT_VERTEX_AGG);
		BooleanWritable updated = getAggregatedValue(VERTEX_UPDATED_AGG);
		logger.info("#### Log custom HERE");
		logger.info(String.format("#### Phases : %d %d %d %d",PHASE_FORWARD, PHASE_BACKWARD, PHASE_CHECK, PHASE_REVERSE_ADD));
		logger.info(String.format("#### At superstep [%d] in phase [%d], current vertex [%d] vertex updated during step : [%b]",getSuperstep(), phase, current.get() ,updated.get()));
		logToCommandLine("SCC Master at superstep " + getSuperstep()+", current phase "+phase+"\n");

		//Start by determining a root node
		if (getSuperstep() == 0) {
			logger.info(String.format("#### At superstep [%d] launch root node designation", getSuperstep()));
			setComputation(SCCCheckComponentComputation.class);
			phase = PHASE_CHECK;
		}
		//If the previous phase was root node determination, check that at least one has been candidate
		//If true, launch forward propagation phase
		else if(phase == PHASE_CHECK && updated.get()){
			logger.info(String.format("#### At superstep [%d] launch forwarding", getSuperstep()));
			setComputation(SCCForwardComputation.class);
			phase = PHASE_FORWARD;
		}
		//If no candidate, algorithm is over
		else if(phase == PHASE_CHECK){
			logger.info(String.format("#### At superstep [%d] no more candidate root node, halt computation SUCCESS", getSuperstep()));
			haltComputation();//Every vertex has been classified
		}
		//If the previous phase was forward propagation, check if one node has been updated
		//If true, continue propagation
		else if(phase == PHASE_FORWARD && updated.get()){
			logger.info(String.format("#### At superstep [%d] keep forwarding", getSuperstep()));
			setComputation(SCCForwardComputation.class);
		}
		//Else, start graph reversion
		//Delete edges
		else if(phase == PHASE_FORWARD){
			logger.info(String.format("#### At superstep [%d] launch reversion-del from forward", getSuperstep()));
			setComputation(SCCEdgesDeletionComputation.class);
			phase = PHASE_REVERSE_DELETE;
		}
		//Rebuild revert edges
		else if(phase == PHASE_REVERSE_DELETE){
			logger.info(String.format("#### At superstep [%d] launch reversion-add from forward", getSuperstep()));
			setComputation(SCCRebuildRevertEdgesComputation.class);
			phase = PHASE_REVERSE_ADD;
		}
		//
		else if(phase == PHASE_REVERSE_ADD){
			logger.info(String.format("#### At superstep [%d] launch backward propagation", getSuperstep()));
			setComputation(SCCBackwardComputation.class);
			phase = PHASE_BACKWARD;
		}
		//If the previous phase was backward propagation, check if one node has been updated
		//If true, continue propagation
		else if(phase == PHASE_BACKWARD && updated.get()){
			logger.info(String.format("#### At superstep [%d] keep backwarding", getSuperstep()));
			setComputation(SCCBackwardComputation.class);
		}
		//Else, start graph reversion-back
		//Delete edges
		else if(phase == PHASE_BACKWARD){
			logger.info(String.format("#### At superstep [%d] launch reversion-del from forward", getSuperstep()));
			setComputation(SCCEdgesDeletionComputation.class);
			phase = PHASE_REVERSE_DELETE_BACK;
		}
		//Rebuild revert edges
		else if(phase == PHASE_REVERSE_DELETE_BACK){
			logger.info(String.format("#### At superstep [%d] launch reversion-add from forward", getSuperstep()));
			setComputation(SCCRebuildRevertEdgesComputation.class);
			phase = PHASE_REVERSE_ADD_BACK;
		}
		//Designate new root node
		else if(phase == PHASE_REVERSE_ADD_BACK){
			logger.info(String.format("#### At superstep [%d] designate new root node", getSuperstep()));
			setAggregatedValue(CURRENT_VERTEX_AGG, new LongWritable(Long.MAX_VALUE));//Reset current vertex
			setComputation(SCCCheckComponentComputation.class);
			phase = PHASE_CHECK;
		}
		//Safety else, should not happen
		else{
			logger.info(String.format("#### At superstep [%d] halt computation ERROR", getSuperstep()));
			haltComputation();
		}

	}
}