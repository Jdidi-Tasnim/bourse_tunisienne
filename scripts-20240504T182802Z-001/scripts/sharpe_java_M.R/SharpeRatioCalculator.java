import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class SharpeRatioCalculator {

    public static class SharpeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text companyName = new Text();
        private DoubleWritable dailyReturn = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            // Assuming the structure of your input is: DATE, GROUPE, CODE, VALEUR, OUVERTURE, CLOTURE, PLUS_BAS, PLUS_HAUT, QUANTITE_NEGOCIEE, NB_TRANSACTION, CAPITAUX, IND_RES
            if (tokens.length >= 6) { // Ensure there are at least 6 columns in the input
                String name = tokens[3]; // Assuming the company name is in the fourth column
                String openStr = tokens[4];
                String closeStr = tokens[5];

                // Check if both opening and closing prices are valid (not "\N")
                if (!openStr.equals("\\N") && !closeStr.equals("\\N")) {
                    double open = Double.parseDouble(openStr); // Assuming opening price is in the fifth column
                    double close = Double.parseDouble(closeStr); // Assuming closing price is in the sixth column
                    double dailyReturnValue = (close - open) / open; // Daily return calculation
                    companyName.set(name);
                    dailyReturn.set(dailyReturnValue);
                    context.write(companyName, dailyReturn);
                }
            }
        }
    }

    public static class SharpeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

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

            // Calculate the standard deviation of daily returns
            double stdDev = Math.sqrt(sumSquaredDifferences / count);

            // Assuming risk-free rate as 0.05
            double riskFreeRate = 0.05;
            // Calculate the Sharpe ratio
            double sharpeRatio = (meanReturn - riskFreeRate) / stdDev;

            context.write(key, new DoubleWritable(sharpeRatio));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sharpe Ratio calculator");
        job.setJarByClass(SharpeRatioCalculator.class);
        job.setMapperClass(SharpeMapper.class);
        job.setReducerClass(SharpeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
