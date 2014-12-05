package org.apache.giraph.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.IntArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

public class StochasticWCCVertex extends Vertex <IntWritable, IntWritable, NullWritable, IntWritable>{

	private boolean inactive;
	private boolean wants_to_propogate;
	@Override
	public void compute(Iterable<IntWritable> messages) throws IOException {
		if(!this.inactive){
			if(getSuperstep() == 0){
				setValue(new IntWritable(Integer.MAX_VALUE));
			}
			else if(getAggregatedValue(StochasticWCCMaster.PHASE_AGGREGATOR).equals(StochasticWCCMaster.TRAVERSAL_SELECTION_1)){
				this.wants_to_propogate = false;
				//the first time a vertex becomes inactive we update the count
				if(!this.inactive && getValue().get() != Integer.MAX_VALUE){
					this.inactive = true;
					aggregate(StochasticWCCMaster.INACTIVE_COUNT_AGGREGATOR, new IntWritable(1));
				}
				else if(!this.inactive){
					//calculate proper p based off of k
					int k = StochasticWCCMaster.K.get(getConf());
					Random r = new Random();
					IntWritable sleeping = getAggregatedValue(StochasticWCCMaster.INACTIVE_COUNT_AGGREGATOR);
					double p = (double) k / (Math.max(getTotalNumVertices() - sleeping.get(), 1));
					if(r.nextDouble() <= p){
						this.wants_to_propogate = true;
						aggregate(StochasticWCCMaster.PROP_AGGREGATOR, new IntArrayWritable(getId().get()));
					}
					
				}
			}
			else if(getAggregatedValue(StochasticWCCMaster.PHASE_AGGREGATOR).equals(StochasticWCCMaster.TRAVERSAL_SELECTION_2)){
				//if we voted to propagate on the last iteration
				if(this.wants_to_propogate){
					IntArrayWritable choices = getAggregatedValue(StochasticWCCMaster.PROP_AGGREGATOR);
					ArrayList<Integer> l = choices.getArrayList();
					int id = getId().get();
					int upper = Math.min(l.size(), StochasticWCCMaster.K.get(getConf()));
					for(int i = 0; i < upper; i++){
						if(l.get(i) == id){
							//start propagating
							sendMessageToAllEdges(getId());
							setValue(getId());
							break;
						}
					}
				}
			}
			else if(getAggregatedValue(StochasticWCCMaster.PHASE_AGGREGATOR).equals(StochasticWCCMaster.TRAVERSAL_MAIN)){
				boolean changed = false;
				for(IntWritable m : messages){
					if(m.get() < getValue().get()){
						changed = true;
						setValue(m);
					}
				}
				if(changed){
					aggregate(StochasticWCCMaster.TRAVERSAL_AGGREGATOR, new BooleanWritable(true));
				}
			}
		}
		else{
			voteToHalt();
		}
		
	}

}
