import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.commons.codec.digest.Blake3;
import scala.Tuple2;

import java.io.Serializable;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class SparkVault implements Serializable {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: SparkVault <k> <numPartitions> <outputPath>");
            System.exit(-1);
        }

        int k = Integer.parseInt(args[0]);
        int numPartitions = Integer.parseInt(args[1]);
        String outputPath = args[2];

        long totalRecords = (1L << k);
        long basePerPartition = totalRecords / numPartitions;
        long remainder = totalRecords % numPartitions;

        SparkConf conf = new SparkConf()
            .setAppName("Spark Vault HashGen and Sort")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryoserializer.buffer.max", "512m");

        JavaSparkContext sc = new JavaSparkContext(conf);

        long startTime = System.currentTimeMillis();

        // Create partitions (distribute remainder)
        List<Long> partitionSizes = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            long size = basePerPartition + (i < remainder ? 1 : 0);
            partitionSizes.add(size);
        }

        // Generate hashes in parallel
        JavaRDD<byte[]> hashRDD = sc.parallelize(partitionSizes, numPartitions)
            .flatMap(numRecords -> {
                List<byte[]> records = new ArrayList<>();
                SecureRandom random = new SecureRandom();
                Blake3 blake3 = Blake3.initHash();

                for (long i = 0; i < numRecords; i++) {
                    byte[] nonce = new byte[6];
                    random.nextBytes(nonce);

                    blake3.reset();
                    blake3.update(nonce);
                    byte[] hash = blake3.doFinalize(10);

                    byte[] record = new byte[16];
                    System.arraycopy(hash, 0, record, 0, 10);
                    System.arraycopy(nonce, 0, record, 10, 6);

                    records.add(record);
                }

                return records.iterator();
            })
            .cache(); // Cache in memory for RDD operations

        // Sort the hashes
        JavaPairRDD<byte[], Integer> keyed = hashRDD
            .mapToPair(record -> new Tuple2<>(record, 1));

        JavaPairRDD<byte[], Integer> sortedRDD =
            keyed.sortByKey(new ByteArrayComparator(), true, numPartitions);

        // Convert to Hadoop writable types and save as SequenceFile
        JavaPairRDD<BytesWritable, NullWritable> outRDD = sortedRDD.mapToPair(
            t -> new Tuple2<>(new BytesWritable(t._1()), NullWritable.get())
        );

        // Use the underlying Hadoop configuration from Spark
        outRDD.saveAsNewAPIHadoopFile(
            outputPath,
            BytesWritable.class,
            NullWritable.class,
            SequenceFileOutputFormat.class,
            sc.hadoopConfiguration()
        );


        long endTime = System.currentTimeMillis();

        System.out.println("Total records generated: " + totalRecords);
        System.out.println("Total execution time: " + (endTime - startTime) / 1000.0 + " seconds");

        sc.stop();
    }

    static class ByteArrayComparator implements java.util.Comparator<byte[]>, Serializable {
        @Override
        public int compare(byte[] a, byte[] b) {
            for (int i = 0; i < Math.min(a.length, b.length); i++) {
                int cmp = Integer.compare(a[i] & 0xFF, b[i] & 0xFF);
                if (cmp != 0) return cmp;
            }
            return Integer.compare(a.length, b.length);
        }
    }
}