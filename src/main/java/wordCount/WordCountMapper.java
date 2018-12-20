package wordCount;

import java.io.IOException; 
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper  extends Mapper<Object, Text, Text, IntWritable>{ 
	/*第一个参数类型是Object，表示输入键key的参数类型(第一个参数 Object是Hadoop根据默认值生成的，
	 * 一般是文件块里的一行文字的行偏移数，这些 ”偏移 数不重要，在处理时候一般用不上)
	 *第二个参数参数类型是Text，表示输 入值的类型第二个参数类型是要处理的字符串，形如 This is a cat.”
	 *第三个参数类型也是Text，表示输出键类型(This”就是第三个参数类型，是Text类型)
	 *第四个参数类型是IntWritable，表示输出值类型。 (1就是第四个参数类 型，是IntWritable)
	 * */
	IntWritable one = new IntWritable(1);  //定义输出值始终是1
	Text word = new Text();   //定义输出形式key的形式
	public void map(Object key, Text value, Context context)throws IOException, InterruptedException {
		/*key是输入键，它是什么无所谓，实际上用不到它的
		 * value是输入值
		 *context输出的形式 */
		StringTokenizer itr = new StringTokenizer(value.toString());//，输入值是Text，需要转换
		while (itr.hasMoreTokens()){      
			word.set(itr.nextToken()); //把输出的可以保存起来
			//System.out.printf("%d\n",one);
			context.write(word, one); //以形式<word,1>保存  
		}  
    }
}