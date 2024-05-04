import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class Moyenne {
    
    public static class MoyenneMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text date = new Text();
        private DoubleWritable variation = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length >= 6 && tokens[2].equals("TUNINDEX")) {
                try {
                    date.set(tokens[0]);
                    variation.set(Double.parseDouble(tokens[5]));
                    context.write(date, variation);
                } catch (NumberFormatException e) {
                    // Handle parsing exception
                }
            }
        }
    }
    
    public static class MoyenneReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            if (count > 0) {
                double mean = sum / count;
                result.set(mean);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Moyenne <inputPath> <outputPath>");
            System.exit(1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(Moyenne.class);
        job.setMapperClass(MoyenneMapper.class);
        job.setReducerClass(MoyenneReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
