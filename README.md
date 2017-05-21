#### Task.java ####

package mapreduce.demo.task1;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Task1 {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "DemoTask1");
		job.setJarByClass(Task1.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setPartitionerClass(Partition.class);
		job.setNumReduceTasks(4);
		job.setMapperClass(Task1Mapper.class);
		job.setReducerClass(Task1Reducer.class);
		 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		/*
		Path out=new Path(args[1]);
		out.getFileSystem(conf).delete(out);
		*/
		
		job.waitForCompletion(true);
	}
}

#### TaskMapper.java ####

package mapreduce.demo.task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Task1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\\|");
				 Text company = new Text("Null");
				 for(int i=0; i<lineArray.length;i++)
				 {
				 company = new Text(lineArray[i]);
		         context.write(company,new IntWritable(1));
			}
		}
}

#### TaskReducer.java ####


package mapreduce.demo.task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>
{	
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		int sum = 0;
		if(!key.toString().equals("Null"))
		{
	    	
		for (IntWritable value : values) {
            
			sum = sum + value.get() ;
		}
	}

		context.write(key, new IntWritable(sum));
	}
}

#### Partition.java ####

package mapreduce.demo.task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Partitioner;

public class Partition extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions)
	{			
		String str = key.toString();
		
		if(numPartitions == 0)
		{
			return 0;
		}
		if(str.toLowerCase().charAt(0) >= 'a' && str.toLowerCase().charAt(0) <= 'f')
		{
			return 0;
		}
		else if(str.toLowerCase().charAt(0) >= 'g' && str.toLowerCase().charAt(0) <= 'l')
		{
			return 1;
		}
		else if(str.toLowerCase().charAt(0) >= 'm' && str.toLowerCase().charAt(0) <= 'r')
		{
			return 2;
		}
		else
		{
			return 3;
		}
	}
}
