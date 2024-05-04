import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EcartType {
    
    public static class EcartTypeMapper extends Mapper<Object, Text, Text, DoubleWritable> {
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
    
    public static class EcartTypeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            double sumOfSquares = 0;
            for (DoubleWritable val : values) {
                double variation = val.get();
                sum += variation;
                sumOfSquares += variation * variation;
                count++;
            }
            if (count > 0) {
                double mean = sum / count;
                double variance = (sumOfSquares / count) - (mean * mean);
                double standardDeviation = Math.sqrt(variance);
                result.set(standardDeviation);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: EcartType <inputPath> <outputPath>");
            System.exit(1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(EcartType.class);
        job.setMapperClass(EcartTypeMapper.class);
        job.setReducerClass(EcartTypeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
