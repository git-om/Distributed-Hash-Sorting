import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.commons.codec.digest.Blake3;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;

public class HadoopVault {

    public static class HashGenMapper extends Mapper<Object, org.apache.hadoop.io.Text, BytesWritable, NullWritable> {
        private SecureRandom random;
        private Blake3 blake3;

        @Override
        protected void setup(Context context) {
            random = new SecureRandom();
            // Blake3.initHash() creates a hash-mode instance :contentReference[oaicite:0]{index=0}
            blake3 = Blake3.initHash();
        }


        @Override
        public void map(Object key, org.apache.hadoop.io.Text value, Context context) 
                throws IOException, InterruptedException {
            
            long numRecords = Long.parseLong(value.toString().trim());
            
            for (long i = 0; i < numRecords; i++) {
                // Generate 6-byte random nonce
                byte[] nonce = new byte[6];
                random.nextBytes(nonce);
                
                // Generate Blake3 hash
                // Generate Blake3 hash (10 bytes)
                blake3.reset();
                blake3.update(nonce);
                byte[] hash = blake3.doFinalize(10);

                
                // Create 16-byte record: 10-byte hash + 6-byte nonce
                byte[] record = new byte[16];
                System.arraycopy(hash, 0, record, 0, 10);
                System.arraycopy(nonce, 0, record, 10, 6);
                
                context.write(new BytesWritable(record), NullWritable.get());
            }
        }
    }

    public static class SortReducer extends Reducer<BytesWritable, NullWritable, BytesWritable, NullWritable> {
        @Override
        public void reduce(BytesWritable key, Iterable<NullWritable> values, Context context) 
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: HadoopVault <k> <numReducers> <inputPath> <outputPath>");
            System.exit(-1);
        }

        int k = Integer.parseInt(args[0]);
        int numReducers = Integer.parseInt(args[1]);
        String inputPath = args[2];
        String outputPath = args[3];

        // We no longer need totalRecords here for input splitting
        Configuration conf = new Configuration();

        // One line from the input file per map task
        // Each line contains the number of records that mapper should generate.
        conf.setLong("mapreduce.input.lineinputformat.linespermap", 1);

        
        Job job = Job.getInstance(conf, "Hadoop Vault HashGen and Sort");
        job.setJarByClass(HadoopVault.class);
        
        job.setMapperClass(HashGenMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(numReducers);
        
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(NullWritable.class);
        
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        
        System.out.println("Total execution time: " + (endTime - startTime) / 1000.0 + " seconds");
        
        System.exit(success ? 0 : 1);
    }
}