package lab1;

import java.io.IOException;
import java.time.Month;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Standard {

	private static Double minRate = 100.0;
	private static Double maxRate = 0.0; 
	
	private static String dateStandard(String date) {
		if (date.contains("/")) {		// 2019/1/1
			String birth_new = date.replace("/", "-");
			return birth_new;
		} else if (date.contains(",")) {	// March 1, 2019
			String[] strs = date.split(",");
			String year = strs[1];
			String[] strs0 = strs[0].split(" ");
			String month = Month.valueOf(strs0[0].toUpperCase()).getValue()+"";
			String day = strs0[1];
			return year+"-"+month+"-"+day;
		} else {
			return date;
		}
	}
	
	private static String temperatureStandard(String temperature) {
		if (temperature.charAt(temperature.length()-1) == '℉') {		// 华氏度
			int len = temperature.length();
			String valString = temperature.substring(0, len-1);
			double tprval = Double.parseDouble(valString);
			double newval = (tprval-32) / 1.8;
			String temperature_new = String.format("%.1f", newval) + '℃';
			return temperature_new;
		} else {
			return temperature;
		}
	}
	
	private static String constructString(String[] tokens) {
		String string = "";
		for (int i = 0; i < tokens.length; i++) {
			string = string + tokens[i] + "|";
		}
		return string.substring(0, string.length()-1);
	}
	
	
	public static class Map extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("\\|");
			String user_birthday = tokens[8];
			tokens[8] = dateStandard(user_birthday);
			String review_date = tokens[4];
			tokens[4] = dateStandard(review_date);
			String temperature = tokens[5];
			tokens[5] = temperatureStandard(temperature);
			String rateString = tokens[6];
			if (!rateString.contains("?")) {
				Double rate = Double.parseDouble(rateString);
				if (rate < minRate)	minRate = rate;
				if (rate > maxRate)	maxRate = rate;
			}
			String textString = constructString(tokens);
			context.write(new Text("1"), new Text(textString));
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context) 
				throws IOException, InterruptedException {
			for (Text val: value) {
				String line = val.toString();
				String[] tokens = line.split("\\|");
				String rateString = tokens[6];
				try {
					Double rate = Double.parseDouble(rateString);
					if (rate >= minRate && rate <= maxRate) {
						tokens[6] = String.format("%.2f", (rate-minRate)/(maxRate-minRate));
						String string = constructString(tokens);
						context.write(null, new Text(string));
					}
				} catch (Exception e) {
					context.write(null, val);
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "standard");
		job.setJarByClass(Standard.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/output/D_Filtered/part-r-00000"));
        Path outPath = new Path("hdfs://localhost:9000/output/D_Standard");
        FileSystem fs = outPath.getFileSystem(conf);
        fs.delete(outPath, true);
        FileOutputFormat.setOutputPath(job, outPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
