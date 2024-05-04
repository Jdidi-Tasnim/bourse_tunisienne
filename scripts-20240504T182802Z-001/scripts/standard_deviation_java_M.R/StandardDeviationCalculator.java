import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class StandardDeviationCalculator {

    public static class StdDevMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text companyName = new Text();
        private DoubleWritable closingPrice = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            // Assuming the structure of your input is: DATE, GROUPE, CODE, VALEUR, OUVERTURE, CLOTURE, PLUS_BAS, PLUS_HAUT, QUANTITE_NEGOCIEE, NB_TRANSACTION, CAPITAUX, IND_RES
            String name = tokens[3]; // Assuming the company name is in the fourth column
            double close = Double.parseDouble(tokens[5]); // Assuming closing price is in the sixth column
            companyName.set(name);
            closingPrice.set(close);
            context.write(companyName, closingPrice);
        }
    }

    public static class StdDevReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            List<Double> prices = new ArrayList<>();
            double sum = 0.0;
            double sumOfSquares = 0.0;
            int count = 0;

            // Compute the sum and sum of squares of prices
            for (DoubleWritable value : values) {
                double price = value.get();
                sum += price;
                sumOfSquares += price * price;
                count++;
                prices.add(price);
            }

            // Calculate the mean
            double mean = sum / count;

            // Calculate the variance
            double variance = (sumOfSquares / count) - (mean * mean);

            // Calculate the standard deviation
            double stdDev = Math.sqrt(variance);

            context.write(key, new DoubleWritable(stdDev));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Standard Deviation calculator");
        job.setJarByClass(StandardDeviationCalculator.class);
        job.setMapperClass(StdDevMapper.class);
        job.setReducerClass(StdDevReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
