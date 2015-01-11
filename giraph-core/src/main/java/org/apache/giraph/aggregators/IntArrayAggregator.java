package org.apache.giraph.aggregators;

import org.apache.giraph.utils.IntArrayWritable;

/**
 * Class for aggregating an array of integers
 * @author Walker Ravina
 *
 */
public class IntArrayAggregator extends BasicAggregator<IntArrayWritable> {

	@Override
	public void aggregate(IntArrayWritable value) {
		getAggregatedValue().getArrayList().addAll(value.getArrayList());
		
	}

	@Override
	public IntArrayWritable createInitialValue() {
		// TODO Auto-generated method stub
		return new IntArrayWritable();
	}

}
