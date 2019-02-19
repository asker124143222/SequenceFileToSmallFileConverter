import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextRecordReader;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @Author: xu.dm
 * @Date: 2019/2/18 21:39
 * @Description:
 */
public class SequenceFileToSmallFileConverter {

    public static void main(String[] args) throws Exception{

        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();


        Path outPath = new Path(args[1]);
        FileSystem fileSystem = outPath.getFileSystem(conf);
        //删除输出路径
        if(fileSystem.exists(outPath))
        {
            fileSystem.delete(outPath,true);
        }

        Job job = Job.getInstance(conf,"SequenceFileToSmallFileConverter");
        job.setJarByClass(SequenceFileToSmallFileConverter.class);

        job.setMapperClass(MultiOutputMapper.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        //TextOutputFormat会在每行文本后面加入换行符号，如果是这个文本作为一个整体来处理，最后就会比预期多一个换行符号
//        job.setOutputFormatClass(TextOutputFormat.class);

        //WholeTextOutputFormat与TextOutputFormat的区别就是没有在每行写入换行符
        job.setOutputFormatClass(WholeTextOutputFormat.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BytesWritable.class);



        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));


        int exitCode = job.waitForCompletion(true) ? 0:1;

        long endTime = System.currentTimeMillis();
        long timeSpan = endTime - startTime;
        System.out.println("运行耗时："+timeSpan+"毫秒。");

        System.exit(exitCode);
    }
}
