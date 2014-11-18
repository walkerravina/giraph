package org.apache.giraph.examples;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.TextAppendAggregator;
import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
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
	//parts 1 and 2 of the consistency check
	public static final IntWritable CHECK_CONSISTENCY_1 = new IntWritable(3);
	public static final IntWritable CHECK_CONSISTENCY_2 = new IntWritable(4);
	public static final IntWritable KMEDOIDS = new IntWritable(5);
	public static final IntWritable HALT = new IntWritable(6);
	
	/**
	 * lambda, the dampening factor
	 */
	public static final FloatConfOption LAMBDA = new FloatConfOption("APVertex.Lambda", (float)0.5);
	/**
	 * the max number of iterations to run for
	 */
	public static final LongConfOption  ITERATIONS = new LongConfOption("APVertex.MaxIterations", 70);
	/**
	 * determines whether to check convergence on every even iteration
	 * if so the algorithm will terminate after convergence or max iterations is exceeded
	 * TODO: Add support for this
	 **/
	public static final BooleanConfOption CONVERGANCE = new BooleanConfOption("APVertex.CheckConvergance", false);
	
	/**
	If true will not out put clusters in which c_k != k but there exists i s.t. c_i=k
	in other words, no clusters with out exemplars will be output
	if after max iterations is met, if there is in consistency than a half iteration of k mediods will
	be run with each i s.t. c_i = i as a possible center.
	**/
	public static final BooleanConfOption CONSISTENCY = new BooleanConfOption("APVertex.Consistency", false);
	/**
	 * Aggregator string constants
	 */
	public static final String PHASE_AGGREGATOR = "phase_aggregator";
	public static final String CONSISTENCY_AGGREGATOR = "consistency_aggregator";
	public static final String KMEDOIDS_AGGREGATOR = "kmedoids_aggregator";
	
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
		//if consistency check specified than begin consistency checking
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(ASSIGNMENT) && CONSISTENCY.get(getConf())){
			setAggregatedValue(PHASE_AGGREGATOR, CHECK_CONSISTENCY_1);
		}
		//otherwise we are done
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(ASSIGNMENT) && !CONSISTENCY.get(getConf())){
			setAggregatedValue(PHASE_AGGREGATOR, HALT);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(CHECK_CONSISTENCY_1)){
			setAggregatedValue(PHASE_AGGREGATOR, CHECK_CONSISTENCY_2);
		}
		//see if there were any inconsistencies found, if so run k medoids for a half iteration
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(CHECK_CONSISTENCY_2)
				&& getAggregatedValue(CONSISTENCY_AGGREGATOR).equals(new BooleanWritable(true))){
			setAggregatedValue(PHASE_AGGREGATOR, KMEDOIDS);
		}
		//otherwise we are done
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(CHECK_CONSISTENCY_2)
				&& getAggregatedValue(CONSISTENCY_AGGREGATOR).equals(new BooleanWritable(false))){
			setAggregatedValue(PHASE_AGGREGATOR, HALT);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(KMEDOIDS)){
			setAggregatedValue(PHASE_AGGREGATOR, HALT);
		}
		//toggle between the two main algorithm phases
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
		registerAggregator(CONSISTENCY_AGGREGATOR, BooleanOrAggregator.class);
		registerPersistentAggregator(KMEDOIDS_AGGREGATOR, TextAppendAggregator.class);
	}
}
