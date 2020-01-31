import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.LocalTime;

import java.io.IOException;

public class MainClass {
    private static String bucketOutputPath;
    private static String input;
    private static String output;

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        checkInputArgs(args);
        getInputArgs(args);
        Configuration wordCountConf = new Configuration();
        wordCountConf.setBoolean("col", true);
        final Job wordCountJob = Job.getInstance(wordCountConf, "WordCount");
        String wordCountFilePath = makeWordCountJob(wordCountJob);
        if (wordCountJob.waitForCompletion(true)) {
            System.out.println("First stage - done! :)");
        } else {
            System.out.println("First stage failed :(");
            System.exit(1);
        }

    }

    private static String makeWordCountJob(Job job) throws IOException {
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.MapperClass.class);
        job.setPartitionerClass(WordCount.PartitionerClass.class);
        job.setCombinerClass(WordCount.ReducerClass.class);
        job.setReducerClass(WordCount.ReducerClass.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        return setPaths(job, input);
    }

    private static String setPaths(Job job, String path) throws IOException {
//        FileInputFormat.addInputPath(job, new Path(path));
        String outputPath = bucketOutputPath + job.getJobName();
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return outputPath;
    }

    private static void getInputArgs(String[] args) {
        input = args[0];
        output = args[1];
        bucketOutputPath = output;
        bucketOutputPath = bucketOutputPath.concat("/" + LocalTime.now().toString().replace(":", "-") + "/");
    }

    private static void checkInputArgs(String[] args) {
        if (args.length < 2) {
            System.out.println("please provide input path and output path");
            System.exit(1);
        }
    }
}
