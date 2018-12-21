package sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MyNewKey implements WritableComparable<MyNewKey> {
    long firstNum;
    long secondNum;

    public MyNewKey() {
    }

    public MyNewKey(long first, long second) {
        this.firstNum = first;
        this.secondNum = second;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(firstNum);
        out.writeLong(secondNum);
    }

    public void readFields(DataInput in) throws IOException {
        this.firstNum = in.readLong();
        this.secondNum = in.readLong();
    }

    /*
     * 当key进行排序时会调用以下这个compreTo方法
     */
    public int compareTo(MyNewKey anotherKey) {
        long min = firstNum - anotherKey.firstNum;
        if (min != 0) {
            // 说明第一列不相等，则返回两数之间小的数
            return (int) min;
        } else {
            return (int) (secondNum - anotherKey.secondNum);
        }
    }
}
