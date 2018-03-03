package in.edureka;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class BookPublishFreqMap extends MapReduceBase implements
Mapper<LongWritable, Text, Text, IntWritable> {

	private Text bookYear = new Text(); 

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		
		
		//Converting the record (single line) to String and storing it in a String variable line
		String line = value.toString();
	
		//StringTokenizer is breaking the record (line) into words
		
		StringTokenizer tokenizer = new StringTokenizer(line, ";");
		
		while (tokenizer.hasMoreTokens()) {
			
			tokenizer.nextElement();
			tokenizer.nextElement();
			tokenizer.nextElement();
			
			String yearData = tokenizer.nextElement().toString();
			   
				
		
		int year = 0;
		// In some cases parts[2] is empty filed and no year.
		
		try {
			if(!yearData.isEmpty())
			{
				String temp = yearData.replace("\"", "");
				bookYear.set(temp + ',');
				
				try {
				year = Integer.parseInt(temp); 
				}
			     catch (NumberFormatException  e) {
			    	 
			    	 tokenizer.nextElement();
					 yearData = tokenizer.nextElement().toString();
				  
				} 
			}
			else
			{
				tokenizer.nextElement();
				yearData = tokenizer.nextElement().toString();
				String temp = yearData.replace("\"", "");
				bookYear.set(temp  + ',');
				year = Integer.parseInt(temp); 
			}
			
			// make sure year is valid #
			if(year > 1800)
			{
				output.collect(bookYear, new IntWritable(1));
			}
			else
			{
				//bookYear.set(yearData);
				bookYear.set("Invalid  year");
				//output.collect(bookYear, new IntWritable(1));
				
			}
			
				} catch (NumberFormatException  e) {
			  
		//	String temp = yearData.replace("\"", "");
		    bookYear.set("Invalid  year");
			//bookYear.set(yearData);
					
					
					
			//output.collect(bookYear, new IntWritable(1));
		} 
		
		break;
		
		}
		
		
		
				
	}
}
