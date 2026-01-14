import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class HashSort {

    // Mapper: takes 16-byte records as BytesWritable, emits them as keys
    public static class HashMapper
            extends Mapper<LongWritable, BytesWritable, BytesWritable, NullWritable> {

        private BytesWritable outKey = new BytesWritable();

        @Override
        protected void map(LongWritable key, BytesWritable value, Context context)
                throws IOException, InterruptedException {

            // Hadoop reuses 'value', so we must copy it
            byte[] copy = value.copyBytes();
            outKey.set(copy, 0, copy.length);
            context.write(outKey, NullWritable.get());
        }
    }

    // Reducer: identity reducer, just writes sorted keys out
    public static class HashReducer
            extends Reducer<BytesWritable, NullWritable, BytesWritable, NullWritable> {

        @Override
        protected void reduce(BytesWritable key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            // We don't care about duplicates; just output the key once per value
            for (NullWritable v : values) {
                context.write(key, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: HashSort <input_path> <output_path>");
            System.exit(1);
        }

        // Hadoop configuration
        Configuration conf = new Configuration();

        // Force local MapReduce runner (so we don't need YARN)
        conf.set("mapreduce.framework.name", "local");

        // Tell FixedLengthInputFormat that each record is 16 bytes (10-byte hash + 6-byte nonce)
        FixedLengthInputFormat.setRecordLength(conf, 16);

        Job job = Job.getInstance(conf, "HashSort");
        job.setJarByClass(HashSort.class);

        // Use FixedLengthInputFormat for 16-byte records
        job.setInputFormatClass(FixedLengthInputFormat.class);

        // Mapper & Reducer classes
        job.setMapperClass(HashMapper.class);
        job.setReducerClass(HashReducer.class);
        job.setNumReduceTasks(1); // single sorted output

        // Mapper output types
        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        // Final output types
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(NullWritable.class);

        // Use SequenceFileOutputFormat (binary output)
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Input & output paths (HDFS paths or local paths, but we'll use HDFS)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit job and wait
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 2);
    }
}
