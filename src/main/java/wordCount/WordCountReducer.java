package wordCount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer;

//��������ͣ�����ֵ���ͣ���������ͣ� ���ֵ����
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	  IntWritable result = new IntWritable();
	  /*key�����������
	   *values��һ��ʵ����Iterable�ӿڵı��������԰�������values������� �ɸ�IntWritable������
	   *����ͨ�������ķ�ʽ�������е�ֵ
	   *Context���ͣ���Mapper���Context���Ƶķ�ʽ�� ����Redurer���ڲ�ʵ�ֵġ�
	   *e.g :����Map����֮�󣬵���reduce������ʱ������ ���ݸ� reduce �������ǣ�
	   *key=��This����values=<1>��key = ��is����values=<1, 1>��key = ��a��, values=<1, 1>��
	   *key=��That��, values=<1>��ע�⣬��key = ��is����key=��a����ʱ��values��������1�� */
	  public void reduce(Text key, Iterable<IntWritable> values, Context context)
			  throws IOException, InterruptedException {
		  int sum = 0;    
		  for (IntWritable val : values) {      
			  sum += val.get();    
		  }    
		  result.set(sum);    
		  context.write(key, result);//ÿ��IntWritable���Ƕ�Ӧ��key��Int�ļ��ϣ�����key-value�ķŽ�ȥ  
	  } 
}