package lab1;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class test {                                            
	
	public static class Map extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(" ");
			context.write(new Text(tokens[0]), value);
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		private MultipleOutputs<Text, Text> mos;
		
		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) 
				throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			mos.close();
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context) 
				throws IOException, InterruptedException {
			
			for (Text val: value) {
				mos.write("a", null, val);
				mos.write("b", null, val);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "test");
		job.setJarByClass(test.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/input/README.txt"));
        Path outPath = new Path("hdfs://localhost:9000/output/test");
        FileSystem fs = outPath.getFileSystem(conf);
        fs.delete(outPath, true);
        FileOutputFormat.setOutputPath(job, outPath);
        MultipleOutputs.addNamedOutput(job, "a", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "b", TextOutputFormat.class, Text.class, Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
