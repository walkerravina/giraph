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
import org.apache.hadoop.io.Text;

/**
 * Master Vertex for performing Affinity Propagation clustering on a data set
 * 
 * Implementation of Delbert Dueck's Affinity Propagation algorithm
 * http://www.psi.toronto.edu/index.php?q=affinity%20propagation
 * @author Walker Ravina
 *
 */
public class APMaster extends DefaultMasterCompute{
	
	public static final IntWritable RESPONSIBILITY_UPDATE = new IntWritable(0);
	public static final IntWritable AVAILABILITY_UPDATE = new IntWritable(1);
	public static final IntWritable ASSIGNMENT = new IntWritable(2);
	
	public static final IntWritable CHECK_CONSISTENCY = new IntWritable(3);
	public static final IntWritable KMEDOIDS_1 = new IntWritable(4);
	public static final IntWritable KMEDOIDS_2 = new IntWritable(5);
	public static final IntWritable KMEDOIDS_3 = new IntWritable(6);
	public static final IntWritable HALT = new IntWritable(7);
	
	/**
	 * lambda, the dampening factor
	 */
	public static final FloatConfOption LAMBDA = new FloatConfOption("APVertex.Lambda", (float)0.5);
	/**
	 * the max number of iterations to run for
	 */
	public static final LongConfOption  ITERATIONS = new LongConfOption("APVertex.MaxIterations", 70);
	
	/**
	If true will not out put clusters in which c_k != k but there exists i s.t. c_i=k
	in other words, no clusters with out exemplars will be output.
	If after max iterations is met, if there is any in consistency than a half iteration of k mediods will
	be run with each i s.t. c_i = i as a possible center.
	**/
	public static final BooleanConfOption CONSISTENCY = new BooleanConfOption("APVertex.Consistency", false);
	/**
	 * Aggregator string constants
	 */
	public static final String PHASE_AGGREGATOR = "phase_aggregator";
	public static final String CONSISTENCY_AGGREGATOR = "consistency_aggregator";
	
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
		//if sufficient iterations have happened and we on a phase where we would normally compute responsibilities
		//than we can proceed to assignment
		else if(getSuperstep() >= ITERATIONS.get(getConf()) && getAggregatedValue(PHASE_AGGREGATOR).equals(AVAILABILITY_UPDATE)){
			setAggregatedValue(PHASE_AGGREGATOR, ASSIGNMENT);
		}
		//toggle between the two main algorithm phases
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(RESPONSIBILITY_UPDATE)){
			setAggregatedValue(PHASE_AGGREGATOR, AVAILABILITY_UPDATE);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(AVAILABILITY_UPDATE)){
			setAggregatedValue(PHASE_AGGREGATOR, RESPONSIBILITY_UPDATE);
		}	
		//if consistency check specified than begin consistency checking
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(ASSIGNMENT) && CONSISTENCY.get(getConf())){
			setAggregatedValue(PHASE_AGGREGATOR, CHECK_CONSISTENCY);
		}
		//otherwise we are done
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(ASSIGNMENT) && !CONSISTENCY.get(getConf())){
			setAggregatedValue(PHASE_AGGREGATOR, HALT);
		}
		//see if there were any inconsistencies found, if so run k medoids for a half iteration
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(CHECK_CONSISTENCY)
				&& getAggregatedValue(CONSISTENCY_AGGREGATOR).equals(new BooleanWritable(true))){
			setAggregatedValue(PHASE_AGGREGATOR, KMEDOIDS_1);
		}
		//otherwise we are done
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(CHECK_CONSISTENCY)
				&& getAggregatedValue(CONSISTENCY_AGGREGATOR).equals(new BooleanWritable(false))){
			setAggregatedValue(PHASE_AGGREGATOR, HALT);
		}
		//if we need to clean up inconsistencies then progress through the 3 parts and halt
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(KMEDOIDS_1)){
			setAggregatedValue(PHASE_AGGREGATOR, KMEDOIDS_2);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(KMEDOIDS_2)){
			setAggregatedValue(PHASE_AGGREGATOR, KMEDOIDS_3);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(KMEDOIDS_3)){
			setAggregatedValue(PHASE_AGGREGATOR, HALT);
		}
	}
	
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException{
		//Aggregator for tracking the phases between different parts of the algorithm
		registerPersistentAggregator(PHASE_AGGREGATOR, IntSumAggregator.class);
		//Aggregator for checking the consistency of exemplar choices
		registerAggregator(CONSISTENCY_AGGREGATOR, BooleanOrAggregator.class);
	}
}
