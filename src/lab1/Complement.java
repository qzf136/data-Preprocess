package lab1;

import java.io.IOException;
import java.util.ArrayList;
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

// rating和user_income缺失值填充
public class Complement {
	
	// user_income 缺失值填充，相同国家、职业的平均值
	public static class Map1 extends Mapper<Object, Text, Text, Text> {
		Text nation_careeer = new Text();
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] strs = line.split("\\|");
			String s = strs[9] + " " + strs[10];
			nation_careeer.set(s);
			context.write(nation_careeer, value);
		}
	}
	
	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			List<String> blankStrings = new ArrayList<String>();
			double sum = 0;
			int n = 0;
			for (Text val : value) {
				String line = val.toString();
				String[] strs = line.split("\\|");
				if (strs[11].equals("?")) {
					blankStrings.add(line);
				}
				else {
					sum += Double.parseDouble(strs[11]);
					n++;
					context.write(null, val);
				} 
			}	
			String income = String.format("%.2f", sum/n);
			for (String string : blankStrings) {
				StringBuffer buffer = new StringBuffer();
				String[] strs = string.split("\\|");
				strs[11] = income;
				for (String str : strs) {
					buffer.append(str + "|");
				}
				String textString = new String(buffer.deleteCharAt(buffer.length()-1));
				context.write(null, new Text(textString));
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
			String[] strs = line.split("\\|");
			String s = strs[1] + " " + strs[2] + " " + strs[3];
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
				String[] strs = line.split("\\|");
				try {
					Double.parseDouble(strs[6]);
					list.add(strs[11] + " " + strs[6]);
					context.write(null, val);
				} catch (Exception e) {
					valueStrings.add(line);
				}
			}
			for (String val: valueStrings) {
				String line = val.toString();
				String[] strs = line.split("\\|");
				int n = 0;
				double sum = 0;				
				for (String s : list) {
					String[] ss = s.split(" ");
					double income = Double.parseDouble(strs[11]);
					if (Math.abs(Double.parseDouble(ss[0]) - income) < 50) {
						sum += Double.parseDouble(ss[1]);
						n += 1;
					}
				}
				strs[6] = sum/n + "";
				StringBuffer buffer = new StringBuffer();
				for (String string : strs) {
					buffer.append(string + "|");
				}				
				String textString = new String(buffer.deleteCharAt(buffer.length()-1));
				context.write(null, new Text(textString));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "complement");
		job1.setJarByClass(Complement.class);
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path("hdfs://localhost:9000/output/D_Standard/part-r-00000"));
        Path outPath1 = new Path("hdfs://localhost:9000/output/D_Preprocessed1");
        FileSystem fs = outPath1.getFileSystem(conf);
        fs.delete(outPath1, true);
        FileOutputFormat.setOutputPath(job1, outPath1);
        
        Job job2 = Job.getInstance(conf, "complement");
	    job2.setJarByClass(Complement.class);
	    job2.setMapperClass(Map2.class);
	    job2.setReducerClass(Reduce2.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job2, new Path("hdfs://localhost:9000/output/D_Preprocessed1/part-r-00000"));
	    FileOutputFormat.setOutputPath(job2, new Path("hdfs://localhost:9000/output/D_Preprocessed2"));
	    Path outPath2 = new Path("hdfs://localhost:9000/output/D_Preprocessed2");
	    FileSystem fs2 = outPath1.getFileSystem(conf);
        fs2.delete(outPath2, true);
        FileOutputFormat.setOutputPath(job2, outPath2);
	    if (job1.waitForCompletion(true)) {
	    	System.exit(job2.waitForCompletion(true) ? 0 : 1);
	    }
	}
}
