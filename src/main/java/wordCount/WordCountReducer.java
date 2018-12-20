package wordCount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer;

//输入键类型，输入值类型，输出键类型， 输出值类型
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	  IntWritable result = new IntWritable();
	  /*key是输入键类型
	   *values是一个实现了Iterable接口的变量，可以把它理解成values里包含若 干个IntWritable整数，
	   *可以通过迭代的方式遍历所有的值
	   *Context类型，跟Mapper里的Context类似的方式， 是在Redurer类内部实现的。
	   *e.g :经过Map过程之后，到达reduce函数的时候，依次 传递给 reduce 函数的是：
	   *key=”This”，values=<1>；key = “is”，values=<1, 1>；key = “a”, values=<1, 1>；
	   *key=”That”, values=<1>。注意，在key = “is”和key=”a”的时候，values里有两个1。 */
	  public void reduce(Text key, Iterable<IntWritable> values, Context context)
			  throws IOException, InterruptedException {
		  int sum = 0;    
		  for (IntWritable val : values) {      
			  sum += val.get();    
		  }    
		  result.set(sum);    
		  context.write(key, result);//每个IntWritable都是对应的key的Int的集合，所以key-value的放进去  
	  } 
}