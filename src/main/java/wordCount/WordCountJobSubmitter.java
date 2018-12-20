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
		 Configuration conf = new Configuration();//ʵ��������Hadoop�������ļ����ȡ����
		 //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();//args�����в���
		 //if (otherArgs.length != 2) {//�жϲ����ĸ����Ƿ���ȷ 
		   //	System.err.println("Usage: wordcount <in> <out>");      
	    	//System.exit(2);    
		 //}
		 Job job = new Job(conf, "wordcount");//job_name = "wordcount"    
		 job.setJarByClass(WordCountJobSubmitter.class);//����    
		 job.setMapperClass(WordCountMapper.class);  //����  
		 job.setReducerClass(WordCountReducer.class);   //����
		 job.setOutputKeyClass(Text.class);    //���
		 job.setOutputValueClass(IntWritable.class); //���   
		 System.out.println("1");
		 FileInputFormat.addInputPath(job, new Path("hdfs://192.168.126.130:9000/user/nancy/readme.txt"));//�����ļ�    
		 FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.126.130:9000/user/result/wordcount/try2"));//����ļ�    
		 System.out.println("2");
		 System.exit(job.waitForCompletion(true) ? 0 : 1);//��ִ����ϣ��˳�
		 
	 }
}

