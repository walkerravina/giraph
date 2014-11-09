package org.apache.giraph.examples;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;

/**
 * Master Vertex for performing Affinity Propagation clustering on a data set
 * 
 * @author Walker Ravina
 *
 */
public class APMaster extends DefaultMasterCompute{
	
	public static final IntWritable RESPONSIBILITY_UPDATE = new IntWritable(0);
	public static final IntWritable AVAILABILITY_UPDATE = new IntWritable(1);
	public static final IntWritable ASSIGNMENT = new IntWritable(2);
	//lambda, the dampening factor
	public static final FloatConfOption LAMBDA = new FloatConfOption("APVertex.Lambda", (float)0.5);
	//the max number of iterations to run for
	public static final LongConfOption  ITERATIONS = new LongConfOption("APVertex.MaxIterations", 70);
	//determines whether to check convergence on every even iteration
	//if so the algorithm will terminate after convergence or max iterations is exceeded
	//TODO: Add support for this
	public static final BooleanConfOption CONVERGANCE = new BooleanConfOption("APVertex.CheckConvergance", false);
	
	public static final String PHASE_AGGREGATOR = "phase_aggregator";
	
	@Override
	public void compute(){
		if(getSuperstep() == 0){
			//vertexes are hard coded to do special responsibility update on iteration 0
			return;
		}
		else if(getSuperstep() == 1){
			//iteration 1 is the first availability update
			setAggregatedValue(PHASE_AGGREGATOR, AVAILABILITY_UPDATE);
		}
		//if sufficient iterations have happened and we are normally on a phase where we would compute responsibilities
		//than we can proceed to assignment
		else if(getSuperstep() >= ITERATIONS.get(getConf()) && getAggregatedValue(PHASE_AGGREGATOR).equals(AVAILABILITY_UPDATE)){
			setAggregatedValue(PHASE_AGGREGATOR, ASSIGNMENT);
		}
		//toggle between the two phases
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(RESPONSIBILITY_UPDATE)){
			setAggregatedValue(PHASE_AGGREGATOR, AVAILABILITY_UPDATE);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(AVAILABILITY_UPDATE)){
			setAggregatedValue(PHASE_AGGREGATOR, RESPONSIBILITY_UPDATE);
		}
		
	}
	
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException{
		//Aggregator for tracking the phases between different parts of the SCC algorithm
		registerPersistentAggregator(PHASE_AGGREGATOR, IntSumAggregator.class);
	}
}
