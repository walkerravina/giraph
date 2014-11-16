package org.apache.giraph.examples;
import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TrianglesVertex extends Vertex<LongWritable, Text, NullWritable, IntWritable > {

	@Override
	public void compute(Iterable<IntWritable> messages) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
