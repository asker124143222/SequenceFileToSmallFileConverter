import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

/**
 * @Author: xu.dm
 * @Date: 2019/2/19 15:13
 * @Description:
 */
public class WholeTextOutputFormat<K, V> extends FileOutputFormat<K, V> {
    public static String SEPARATOR = "mapreduce.output.textoutputformat.separator";
    /**
     * @deprecated Use {@link #SEPARATOR}
     */
    @Deprecated
    public static String SEPERATOR = SEPARATOR;
    protected static class LineRecordWriter<K, V>
            extends RecordWriter<K, V> {
        private static final byte[] NEWLINE =
                "\n".getBytes(StandardCharsets.UTF_8);

        protected DataOutputStream out;
        private final byte[] keyValueSeparator;

        public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
            this.out = out;
            this.keyValueSeparator =
                    keyValueSeparator.getBytes(StandardCharsets.UTF_8);
        }

        public LineRecordWriter(DataOutputStream out) {
            this(out, "\t");
        }

        /**
         * Write the object to the byte stream, handling Text as a special
         * case.
         * @param o the object to print
         * @throws IOException if the write throws, we pass it on
         */
        private void writeObject(Object o) throws IOException {
            if (o instanceof Text) {
                Text to = (Text) o;
                out.write(to.getBytes(), 0, to.getLength());
            } else {
                out.write(o.toString().getBytes(StandardCharsets.UTF_8));
            }
        }

        public synchronized void write(K key, V value)
                throws IOException {

            boolean nullKey = key == null || key instanceof NullWritable;
            boolean nullValue = value == null || value instanceof NullWritable;
            if (nullKey && nullValue) {
                return;
            }
            if (!nullKey) {
                writeObject(key);
            }
            if (!(nullKey || nullValue)) {
                out.write(keyValueSeparator);
            }
            if (!nullValue) {
                writeObject(value);
            }
//            out.write(NEWLINE);
        }

        public synchronized
        void close(TaskAttemptContext context) throws IOException {
            out.close();
        }
    }

    public RecordWriter<K, V>
    getRecordWriter(TaskAttemptContext job
    ) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        String keyValueSeparator= conf.get(SEPARATOR, "\t");
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass =
                    getOutputCompressorClass(job, GzipCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }
        Path file = getDefaultWorkFile(job, extension);
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, false);
        if (isCompressed) {
            return new WholeTextOutputFormat.LineRecordWriter<>(
                    new DataOutputStream(codec.createOutputStream(fileOut)),
                    keyValueSeparator);
        } else {
            return new WholeTextOutputFormat.LineRecordWriter<>(fileOut, keyValueSeparator);
        }
    }
}
