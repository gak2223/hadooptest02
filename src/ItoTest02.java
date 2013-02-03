
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ItoTest02 {

	/**
	 * Mapper
	 */
	public static class Map
			extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(
				LongWritable key,
				Text value,
				Context context) throws IOException, InterruptedException {

			Text tKey = new Text();
			java.lang.StringBuffer sb = new java.lang.StringBuffer();


			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			for (int i = 0; tokenizer.hasMoreTokens(); i++) {
				String s = tokenizer.nextToken();
				switch (i) {
				case 0:
					//IDは捨てる
					break;
				case 1: //GroupByの条件に使う項目
					tKey.set(s.trim());
					break;
				case 2:
					sb.append(s.trim());
					break;
				case 3:
					sb.append(", ");
					sb.append(s.trim());
					break;
				}
			}
			context.write(tKey, new Text(sb.toString()));
		}
	}

	/**
	 * Reducer
	 */
	public static class Reduce
			extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(
				Text key,
				Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			String[] array = {"", ""};
			long l1 = 0;
			long l2 = 0;
			java.math.BigDecimal sum1 = new java.math.BigDecimal(0);
			java.math.BigDecimal sum2 = new java.math.BigDecimal(0);

			for (Text value : values) {
				array  =  value.toString().split(",");
				l1 = Long.parseLong(array[0].trim());
				l2 = Long.parseLong(array[1].trim());
				sum1 = sum1.add(new java.math.BigDecimal(l1));
				sum2 = sum2.add(new java.math.BigDecimal(l2));
			}
			context.write(key, new Text(sum1.toString() + ", " + sum2.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(ItoTest02.class);
		job.setJobName("itotest02");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}