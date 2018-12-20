package wordCount;

import java.io.IOException; 
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper  extends Mapper<Object, Text, Text, IntWritable>{ 
	/*��һ������������Object����ʾ�����key�Ĳ�������(��һ������ Object��Hadoop����Ĭ��ֵ���ɵģ�
	 * һ�����ļ������һ�����ֵ���ƫ��������Щ ��ƫ�� ������Ҫ���ڴ���ʱ��һ���ò���)
	 *�ڶ�����������������Text����ʾ�� ��ֵ�����͵ڶ�������������Ҫ������ַ��������� This is a cat.��
	 *��������������Ҳ��Text����ʾ���������(This�����ǵ������������ͣ���Text����)
	 *���ĸ�����������IntWritable����ʾ���ֵ���͡� (1���ǵ��ĸ������� �ͣ���IntWritable)
	 * */
	IntWritable one = new IntWritable(1);  //�������ֵʼ����1
	Text word = new Text();   //���������ʽkey����ʽ
	public void map(Object key, Text value, Context context)throws IOException, InterruptedException {
		/*key�������������ʲô����ν��ʵ�����ò�������
		 * value������ֵ
		 *context�������ʽ */
		StringTokenizer itr = new StringTokenizer(value.toString());//������ֵ��Text����Ҫת��
		while (itr.hasMoreTokens()){      
			word.set(itr.nextToken()); //������Ŀ��Ա�������
			//System.out.printf("%d\n",one);
			context.write(word, one); //����ʽ<word,1>����  
		}  
    }
}