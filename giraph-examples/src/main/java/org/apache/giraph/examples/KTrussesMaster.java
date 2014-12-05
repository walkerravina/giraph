package org.apache.giraph.examples;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * Master Class for K Trusses Algorithm implementation in
 * http://delivery.acm.org/10.1145/2460000/2457085/4799a457.pdf?ip=128.61.68.178&id=2457085&acc=NO%20RULES&key=A79D83B43E50B5B8.5E2401E94B5C98E0.4D4702B0C3E38B35.4D4702B0C3E38B35&CFID=605571014&CFTOKEN=48494189&__acm__=1417824601_29c2ea80c5405c62986c42c6ada8683e 
 * @author Walker Ravina
 *
 */
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
