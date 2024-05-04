import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MovingAverage {

    public static class MovingAverageMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length == 11) {
                String date = tokens[0];
                String closingPrice = tokens[5];
                context.write(new Text(date), new Text(closingPrice));
            }
        }
    }

    public static class MovingAverageReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<Double> closingPrices = new ArrayList<>();
            for (Text val : values) {
                closingPrices.add(Double.parseDouble(val.toString()));
            }

            double sum = 0;
            for (double price : closingPrices) {
                sum += price;
            }
            double mean = sum / closingPrices.size();

            double sumOfSquaredDifferences = 0;
            for (double price : closingPrices) {
                sumOfSquaredDifferences += Math.pow(price - mean, 2);
            }
            double standardDeviation = Math.sqrt(sumOfSquaredDifferences / closingPrices.size());

            context.write(key, new Text(String.format("%.2f, %.2f", mean, standardDeviation)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "moving average");
        job.setJarByClass(MovingAverage.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0])); // Input path
        
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1])); // Output path
        
        job.setMapperClass(MovingAverageMapper.class);
        job.setReducerClass(MovingAverageReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
