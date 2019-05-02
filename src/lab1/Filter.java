package lab1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Filter {
	
	private static int lineNum = 0;
	private static java.util.Map<Double, Integer> rateNum = new HashMap<Double, Integer>();
	private static Double smallRate = null;
	private static Double largeRate = null;
	private static int sortCount = 0;
	
	private static void sort() {
		List<Double> rates = new ArrayList<Double>(rateNum.keySet());
		Double[] a = new Double[rates.size()];
		rates.toArray(a);
		Arrays.sort(a);
		System.out.println(Arrays.asList(a));
		int sum1 = 0;
		int i = 0;
		while (sum1 < lineNum/100) {
			sum1 += rateNum.get(a[i]);
			i++;
		}
		int sum2 = 0;
		int j = rates.size()-1;
		while (sum2 < lineNum/100) {
			sum2 += rateNum.get(a[j]);
			j--;
		}
		smallRate = a[i+1];
		largeRate = a[j-1];
	}
	
	
	public static class Map extends Mapper<Object, Text, Text, Text> {
		
		double longitude_1 = 8.1461259, longitude_2 = 11.1993265;
		double latitude_1 = 56.5824856, latitude_2 = 57.750511;
		
		// 过滤longitude 和 latitude 无效数据
		// 记录 rate 的值及这个值的行数
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("\\|");
			Double longitude = Double.parseDouble(tokens[1]);
			Double latitude = Double.parseDouble(tokens[2]);
			String rateString = tokens[6];
			if (longitude >= longitude_1 && longitude <= longitude_2 && latitude >= latitude_1 && latitude <= latitude_2) {
				if (!rateString.contains("?")) {
					double rate_val = Double.parseDouble(rateString);
					lineNum++;
					if (rateNum.get(rate_val) == null) {
						rateNum.put(rate_val, 1);
					} else {
						int num = rateNum.get(rate_val);
						rateNum.put(rate_val, num+1);
					}
				}
				context.write(new Text(""), value);
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context) 
				throws IOException, InterruptedException {
			if (sortCount == 0) {
				sort();
				sortCount++;
				System.out.println("rate_1 = " + smallRate);
				System.out.println("rate_2 = " + largeRate);
			}
			for (Text val: value) {
				String line = val.toString();
				String[] tokens = line.split("\\|");
				String rateString = tokens[6];
				try {
					Double rate = Double.parseDouble(rateString);
					if (rate >= smallRate && rate <= largeRate) {
						context.write(null, val);
					}
				} catch (Exception e) {
					context.write(null, val);
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "filter");
		job.setJarByClass(Filter.class);
		job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPaths(job, "hdfs://localhost:9000/input/large_data_utf8.txt");
	    Path outPath = new Path("hdfs://localhost:9000/output/D_Filtered");
        FileSystem fs = outPath.getFileSystem(conf);
        fs.delete(outPath, true);
        FileOutputFormat.setOutputPath(job, outPath);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
