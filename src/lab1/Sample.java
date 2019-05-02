package lab1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//使用user_career作为分层变量，进行分层抽样
public class Sample {

	private static java.util.Map<String, Integer> careerNum = new HashMap<String, Integer>();
	private static int lineNum = 0;
	
	public static class Map extends Mapper<Object, Text, Text, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] strs = line.split("\\|");
			String career = strs[10];
			lineNum++;
			if (!careerNum.containsKey(career)) {
				careerNum.put(career, 1);
			} else {
				int num = careerNum.get(career);
				careerNum.put(career, num+1);
			}
			context.write(new Text(career), value);
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			String career = key.toString();
			int num = careerNum.get(career);
			int sampleNum = (int) Math.round(100.0 * num/lineNum);
			Random random = new Random();
			List<Integer> indexs = new ArrayList<Integer>();
			while (indexs.size() != sampleNum) {
				int a = random.nextInt(num);
				if (!indexs.contains(a)) {
					indexs.add(a);
				}
			}
			System.out.println(indexs);
			int i = 0;
			for (Text val : value) {
				if (indexs.contains(i)) {
					context.write(null, val);
				}
				i++;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "sample");
		job.setJarByClass(Sample.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/input/large_data_utf8.txt"));
        Path outPath = new Path("hdfs://localhost:9000/output/D_Sample");
        FileSystem fs = outPath.getFileSystem(conf);
        fs.delete(outPath, true);
        FileOutputFormat.setOutputPath(job, outPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
