import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class SortinoRatioCalculator {

    public static class SortinoMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text companyName = new Text();
        private DoubleWritable dailyReturn = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length >= 6) { // Ensure at least 6 tokens are available
                String name = tokens[3]; // Assuming the company name is in the fourth column
                String openStr = tokens[4];
                String closeStr = tokens[5];
                try {
                    double open = Double.parseDouble(openStr);
                    double close = Double.parseDouble(closeStr);
                    double dailyReturnValue = (close - open) / open;
                    companyName.set(name);
                    dailyReturn.set(dailyReturnValue);
                    context.write(companyName, dailyReturn);
                } catch (NumberFormatException e) {
                    // Handle invalid input, such as "\N"
                    // You can choose to ignore this entry or log a message
                    System.err.println("Invalid input for opening or closing price: " + openStr + ", " + closeStr);
                }
            }
        }
    }

    public static class SortinoReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            List<Double> returns = new ArrayList<>();
            double sum = 0.0;
            int count = 0;

            // Compute the sum of daily returns
            for (DoubleWritable value : values) {
                double dailyReturn = value.get();
                sum += dailyReturn;
                count++;
                returns.add(dailyReturn);
            }

            // Calculate the mean daily return
            double meanReturn = sum / count;

            // Compute the sum of squared differences from the mean
            double sumSquaredDifferences = 0.0;
            for (double dailyReturn : returns) {
                sumSquaredDifferences += Math.pow(dailyReturn - meanReturn, 2);
            }

            // Calculate the downside deviation (standard deviation of negative returns)
            double sumSquaredNegativeDifferences = 0.0;
            for (double dailyReturn : returns) {
                if (dailyReturn < 0) {
                    sumSquaredNegativeDifferences += Math.pow(dailyReturn - meanReturn, 2);
                }
            }
            double downsideDeviation = Math.sqrt(sumSquaredNegativeDifferences / count);

            // Calculate the Sortino ratio
            double riskFreeRate = 0.05; // Assuming risk-free rate as 0.05
            double sortinoRatio = (meanReturn - riskFreeRate) / downsideDeviation;

            context.write(key, new DoubleWritable(sortinoRatio));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sortino Ratio calculator");
        job.setJarByClass(SortinoRatioCalculator.class);
        job.setMapperClass(SortinoMapper.class);
        job.setReducerClass(SortinoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
