package sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MySortJob {
	//Maper
	public static class sortMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable>{
		LongWritable firstColumn  = new LongWritable();
		LongWritable secondColumn  = new LongWritable();
		public void map( LongWritable key,Text value,Context context)
				throws IOException, InterruptedException {
			//对按行读取的数，切分
			String[] splitResult = value.toString().split("\t");
			firstColumn.set(Long.parseLong(splitResult[0]));
			secondColumn.set(Long.parseLong(splitResult[1]));
			context.write(firstColumn, secondColumn);
			System.out.println("map");
		}
	}
	//Reducer
	public static class sortReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		public void reduce(LongWritable key, Iterable<LongWritable> values, 
				Context context)
				  throws IOException, InterruptedException {
			for(LongWritable value : values) {
				context.write(key, value);
			}
			System.out.println("reducer");
		}
	}
	//main
	public static void main(String[] args) throws Exception { 
		Configuration conf = new Configuration();
		Job job = new Job(conf,"sort");
		job.setJarByClass(MySortJob.class);
		job.setMapperClass(sortMapper.class);
		job.setReducerClass(sortReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.126.130:9000/user/nancy/word.txt"));
		FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.126.130:9000/user/sort/result/default") );
		System.out.println("1");
		System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
	}
}
