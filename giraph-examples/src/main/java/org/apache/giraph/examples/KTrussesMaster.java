package org.apache.giraph.examples;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

public class KTrussesMaster extends DefaultMasterCompute {

	public static final IntWritable TRIANGLE_QUERY = new IntWritable(0);
	public static final IntWritable TRIANGLE_ANSWER = new IntWritable(1);
	public static final IntWritable COUNT_EDGE_SUPPORT = new IntWritable(2);
	public static final IntWritable REMOVE_NODES = new IntWritable(3);
	public static final IntWritable FIND_COMPONENTS_0 = new IntWritable(4);
	public static final IntWritable FIND_COMPONENTS_1 = new IntWritable(5);
	public static final IntWritable FIND_COMPONENTS_2 = new IntWritable(6);
	
	public static final IntConfOption K = new IntConfOption("KTrussesVertex.K", 5);
	
	public static final String PHASE_AGGREGATOR = "phase_aggregator";
	public static final String EDGE_SUPPORT_AGGREGATOR = "edge_support_aggregator";
	
	public void compute(){
		//we begin on superstep 2
		if(getSuperstep() == 2){
			setAggregatedValue(PHASE_AGGREGATOR, TRIANGLE_QUERY);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(TRIANGLE_QUERY)){
			setAggregatedValue(PHASE_AGGREGATOR, TRIANGLE_ANSWER);
			setAggregatedValue(EDGE_SUPPORT_AGGREGATOR, new BooleanWritable(false));
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(TRIANGLE_ANSWER)){
			setAggregatedValue(PHASE_AGGREGATOR, COUNT_EDGE_SUPPORT);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(COUNT_EDGE_SUPPORT)){
			if(getAggregatedValue(EDGE_SUPPORT_AGGREGATOR).equals(new BooleanWritable(false))){
				setAggregatedValue(PHASE_AGGREGATOR, FIND_COMPONENTS_0);
			}
			else{
				setAggregatedValue(PHASE_AGGREGATOR, REMOVE_NODES);
			}
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(REMOVE_NODES)){
			setAggregatedValue(PHASE_AGGREGATOR, TRIANGLE_QUERY);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(FIND_COMPONENTS_0)){
			setAggregatedValue(PHASE_AGGREGATOR, FIND_COMPONENTS_1);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(FIND_COMPONENTS_1)){
			setAggregatedValue(PHASE_AGGREGATOR, FIND_COMPONENTS_2);
		}
		
	}
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException{
		//Aggregator for tracking the phases between different parts of the KTrusses algorithm
		registerPersistentAggregator(PHASE_AGGREGATOR, IntSumAggregator.class);
		registerPersistentAggregator(EDGE_SUPPORT_AGGREGATOR, BooleanOrAggregator.class);
	}
}
