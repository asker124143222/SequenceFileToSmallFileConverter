import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;


/**
 * @Author: xu.dm
 * @Date: 2019/2/18 21:40
 * @Description:
 */
public class MultiOutputMapper extends Mapper<Text,BytesWritable,NullWritable,Text> {
    private MultipleOutputs<NullWritable,Text> multipleOutputs;
    private long splitLength;

    /**
     * Called once at the beginning of the task.
     *
     * @param context
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
        InputSplit split = context.getInputSplit();
        splitLength = split.getLength();
    }

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     *
     * @param key
     * @param value
     * @param context
     */
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        System.out.printf("split length:%s,value length:%s,bytes length:%s",splitLength,value.getLength(),value.getBytes().length);
        int length = value.getLength();
        byte[] bytes = new byte[length];
        System.arraycopy(value.getBytes(),0,bytes,0,length);

        Text contents = new Text(bytes);
        System.out.println(contents.toString());
        multipleOutputs.write(NullWritable.get(),new Text(bytes),key.toString());
//
//        multipleOutputs.write(NullWritable.get(),new Text(value.getBytes()),key.toString());//这句是错的。
//        通过测试，对于SequenceFile，是按key进入分片，value的length是实际长度,value.getbytes的长度是value的buff长度，两个不一定相等
//        split length:88505,value length:4364,bytes length:6546
    }

    /**
     * Called once at the end of the task.
     *
     * @param context
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
        super.cleanup(context);
    }
}
