package wordCount;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser;
public class WordCountJobSubmitter {
	 public static void main(String[] args) throws Exception { 
		 Configuration conf = new Configuration();//实例化，从Hadoop的配置文件里读取参数
		 //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();//args命令行参数
		 //if (otherArgs.length != 2) {//判断参数的个数是否正确 
		   //	System.err.println("Usage: wordcount <in> <out>");      
	    	//System.exit(2);    
		 //}
		 Job job = new Job(conf, "wordcount");//job_name = "wordcount"    
		 job.setJarByClass(WordCountJobSubmitter.class);//输入    
		 job.setMapperClass(WordCountMapper.class);  //输入  
		 job.setReducerClass(WordCountReducer.class);   //输入
		 job.setOutputKeyClass(Text.class);    //输出
		 job.setOutputValueClass(IntWritable.class); //输出   
		 System.out.println("1");
		 FileInputFormat.addInputPath(job, new Path("hdfs://192.168.126.130:9000/user/nancy/readme.txt"));//输入文件    
		 FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.126.130:9000/user/result/wordcount/try2"));//输出文件    
		 System.out.println("2");
		 System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
		 
	 }
}

