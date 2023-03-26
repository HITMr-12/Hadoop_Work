import com.google.common.base.Preconditions;
import com.sun.org.slf4j.internal.LoggerFactory;
import com.sun.org.slf4j.internal.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;

class MyTextOutputFormat extends TextOutputFormat {
    //private static final Logger LOG = LoggerFactory.getLogger(FileOutputFormat.class);
    private static String fileName="";
    public static void SetFileName(String fileName){
        MyTextOutputFormat.fileName=fileName;
    }
    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
        OutputCommitter c = getOutputCommitter(context);
        Preconditions.checkState(c instanceof PathOutputCommitter,
                "Committer %s is not a PathOutputCommitter", c);
        Path workPath = ((PathOutputCommitter) c).getWorkPath();
        Preconditions.checkNotNull(workPath,
                "Null workPath returned by committer %s", c);
        Path workFile = new Path(workPath,fileName);
        //LOG.debug("Work file for {} extension '{}' is {}", context, extension, workFile);
        return workFile;
    }

    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        String keyValueSeparator= conf.get(SEPARATOR, "");
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
            return new LineRecordWriter<>(
                    new DataOutputStream(codec.createOutputStream(fileOut)),
                    keyValueSeparator);
        } else {
            return new LineRecordWriter<>(fileOut, keyValueSeparator);
        }
    }
}