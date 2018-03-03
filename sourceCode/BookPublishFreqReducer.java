package in.edureka;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;



public  class BookPublishFreqReducer extends MapReduceBase implements
Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
	
	  //Defining a local variable sum of type int
		int sum = 0;
	
	/*
	 * Iterates through all the values available with a key and adds them together 
	 * and give the final result as the key and sum of its values.
	 */
	
		while (values.hasNext()) {
	
			sum += values.next().get();
			
			
		}
		
		//Dumping the output
		output.collect(key, new IntWritable(sum));
		
	}
}