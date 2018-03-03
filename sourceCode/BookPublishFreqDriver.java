package in.edureka;


import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class BookPublishFreqDriver {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		//Creating a JobConf object and assigning a job name for identification purposes
		JobConf conf = new JobConf(BookPublishFreqDriver.class);
		conf.setJobName("BookPublishFreq");

		// Setting configuration object with the Data Type of output Key and
		// Value
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		// Providing the mapper and reducer class names
		conf.setMapperClass(BookPublishFreqMap.class);
		conf.setReducerClass(BookPublishFreqReducer.class);

		// Setting format of input and output
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// The hdfs input and output directory to be fetched from the command
		// line
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		// Running the job
		JobClient.runJob(conf);

	}

}
