package lab1;

import java.io.IOException;
import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class All {
	
	private static int rateLineNum = 0;
	private static int allLineNum = 0;
	private static Map<Double, Integer> rateNum = new HashMap<Double, Integer>();
	private static Map<String, Integer> careerNum = new HashMap<String, Integer>();
	private static Double smallRate = null;
	private static Double largeRate = null;
	private static int sortCount = 0;
	
	private static Map<String, Double> nation_careerIncomeSum = new HashMap<String, Double>();
	private static Map<String, Integer> nation_careerIncomeNum = new HashMap<String, Integer>();
	
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
		if ((int)temperature.charAt(temperature.length()-1) == 72) {		// 华氏度
			int len = temperature.length();
			String valString = temperature.substring(0, len-2);
			double tprval = Double.parseDouble(valString);
			double newval = (tprval-32) / 1.8;
			String temperature_new = String.format("%.1f", newval) + (char)65533 +""+ (char)65533;
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
	
	private static void sort() {
		List<Double> rates = new ArrayList<Double>(rateNum.keySet());
		Double[] a = new Double[rates.size()];
		rates.toArray(a);
		Arrays.sort(a);
		int sum1 = 0;
		int i = 0;
		while (sum1 < rateLineNum/100) {
			sum1 += rateNum.get(a[i]);
			i++;
		}
		int sum2 = 0;
		int j = rates.size()-1;
		while (sum2 < rateLineNum/100) {
			sum2 += rateNum.get(a[j]);
			j--;
		}
		smallRate = a[i+1];
		largeRate = a[j-1];
	}
	
	public static class Map1 extends Mapper<Object, Text, Text, Text> {
		
		double longitude_1 = 8.1461259, longitude_2 = 11.1993265;
		double latitude_1 = 56.5824856, latitude_2 = 57.750511;
		
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
			String textString = constructString(tokens);
			String rate = tokens[6];
			String nation = tokens[9];
			String career = tokens[10];
			String income = tokens[11];
			String nation_career = nation + " " + career;
			Double longitude = Double.parseDouble(tokens[1]);
			Double latitude = Double.parseDouble(tokens[2]);
			if (longitude >= longitude_1 && longitude <= longitude_2 && latitude >= latitude_1 && latitude <= latitude_2) {
				allLineNum++;
				if (!rate.contains("?")) {
					rateLineNum++;
					double rate_val = Double.parseDouble(rate);
					if (rateNum.get(rate_val) == null) {
						rateNum.put(rate_val, 1);
					} else {
						int num = rateNum.get(rate_val);
						rateNum.put(rate_val, num+1);
					}
				}
				if (!careerNum.containsKey(career)) {
					careerNum.put(career, 1);
				} else {
					int num = careerNum.get(career);
					careerNum.put(career, num+1);
				}
				if (!income.contains("?")) {
					double income_val = Double.parseDouble(income);
					if (nation_careerIncomeNum.get(nation_career) == null) {
						nation_careerIncomeNum.put(nation_career, 1);
						nation_careerIncomeSum.put(nation_career, income_val);
					} else {
						int num = nation_careerIncomeNum.get(nation_career);
						nation_careerIncomeNum.put(nation_career, num+1);
						double sum = nation_careerIncomeSum.get(nation_career);
						nation_careerIncomeSum.put(nation_career, sum+income_val);
					}
				}
				context.write(new Text(career), new Text(textString));
			}
		}
	}
	
	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		
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
			if (sortCount == 0) {
				sort();
				sortCount++;
			}
			String career = key.toString();
			int num = careerNum.get(career);
			int sampleNum = (int) Math.round(1000.0 * num/allLineNum);
			Random random = new Random();
			List<Integer> indexs = new ArrayList<Integer>();
			while (indexs.size() != sampleNum) {
				int a = random.nextInt(num);
				if (!indexs.contains(a)) {
					indexs.add(a);
				}
			}
			int i = 0;
			for (Text val: value) {
				String line = val.toString();
				String[] tokens = line.split("\\|");
				String rateString = tokens[6];
				if (!rateString.contains("?")) {	// 归一化
					Double rate = Double.parseDouble(rateString);
					if (rate >= smallRate && rate <= largeRate) {
						tokens[6] = String.format("%.2f", (rate-smallRate)/(largeRate-smallRate));
						if (tokens[11].contains("?")) {
							String nation = tokens[9];
							String nationCareer = nation + " " + career;
							double income = nation_careerIncomeSum.get(nationCareer) / nation_careerIncomeNum.get(nationCareer);
							String incomeString = String.format("%.2f", income);
							tokens[11] = incomeString;
						}
						mos.write("FS", null, new Text(constructString(tokens)));
					}
				} else {
					mos.write("FS", null, val);
				}
				if (indexs.contains(i)) {
					mos.write("sample", null, val);
				}
				i++;
			}
		}
	}

	
	// rating 缺失值填充，相同地点，收入相近的平均值
	public static class Map2 extends Mapper<Object, Text, Text, Text> {
		Text addr = new Text();
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("\\|");
			String longitude = tokens[1].substring(0, tokens[1].indexOf(".")+2);
			String latitude = tokens[2].substring(0, tokens[2].indexOf(".")+2);
			String altitude = tokens[3].substring(0, tokens[3].indexOf(".")+2);
			String s = longitude + " " + latitude + " " + altitude;
			addr.set(s);
			context.write(addr, value);
		}
	}	
	
	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			List<String> valueStrings = new ArrayList<String>();
			List<String> list = new ArrayList<String>();
			for (Text val : value) {
				String line = val.toString();
				String[] tokens = line.split("\\|");
				try {
					Double.parseDouble(tokens[6]);
					list.add(tokens[11] + " " + tokens[6]);
					context.write(null, val);
				} catch (Exception e) {
					valueStrings.add(line);
				}
			}
			for (String val: valueStrings) {
				String line = val.toString();
				String[] tokens = line.split("\\|");
				int n = 0;
				double sum = 0;				
				for (String s : list) {
					String[] income_rate = s.split(" ");	
					double income = Double.parseDouble(tokens[11]);
					if (Math.abs(Double.parseDouble(income_rate[0]) - income) < 500) {
						sum += Double.parseDouble(income_rate[1]);
						n += 1;
					}
				}
				tokens[6] = String.format("%.2f", sum/n);
				String textString = constructString(tokens);
				context.write(null, new Text(textString));
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "all1");
		job1.setJarByClass(All.class);
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path("hdfs://localhost:9000/input/large_data.txt"));
        Path outPath1 = new Path("hdfs://localhost:9000/output/All1");
        FileSystem fs1 = outPath1.getFileSystem(conf);
        fs1.delete(outPath1, true);
        FileOutputFormat.setOutputPath(job1, outPath1);
        MultipleOutputs.addNamedOutput(job1, "FS", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job1, "sample", TextOutputFormat.class, Text.class, Text.class);
        
        Job job2 = Job.getInstance(conf, "all2");
		job2.setJarByClass(Complement.class);
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("hdfs://localhost:9000/output/All1/FS-r-00000"));
        Path outPath2 = new Path("hdfs://localhost:9000/output/All2");
        FileSystem fs2 = outPath2.getFileSystem(conf);
        fs2.delete(outPath2, true);
        FileOutputFormat.setOutputPath(job2, outPath2);
        
        if (job1.waitForCompletion(true)) {
        	System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
	}
}
