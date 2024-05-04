import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class CalmarRatioCalculator {

    public static class CalmarMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text companyName = new Text();
        private DoubleWritable dailyReturn = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length >= 6) { // Ensure there are at least 6 columns in the input
                String companyNameStr = tokens[3];
                String openStr = tokens[4];
                String closeStr = tokens[5];

                if (!openStr.equals("\\N") && !closeStr.equals("\\N")) {
                    double open;
                    double close;
                    try {
                        open = Double.parseDouble(openStr);
                        close = Double.parseDouble(closeStr);
                    } catch (NumberFormatException e) {
                        // Log the error or handle it appropriately
                        return; // Skip this record and proceed to the next one
                    }

                    double dailyReturnValue = (close - open) / open;
                    companyName.set(companyNameStr);
                    dailyReturn.set(dailyReturnValue);
                    context.write(companyName, dailyReturn);
                }
            }
        }
    }

    public static class CalmarReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            List<Double> returns = new ArrayList<>();
            double sum = 0.0;
            int count = 0;

            for (DoubleWritable value : values) {
                double dailyReturn = value.get();
                sum += dailyReturn;
                count++;
                returns.add(dailyReturn);
            }

            double meanReturn = sum / count;
            double maxDrawdown = calculateMaxDrawdown(returns);
            double calmarRatio = meanReturn / Math.abs(maxDrawdown);

            context.write(key, new DoubleWritable(calmarRatio));
        }

        private double calculateMaxDrawdown(List<Double> returns) {
            double maxDrawdown = 0.0;
            double peak = Double.NEGATIVE_INFINITY;

            for (double dailyReturn : returns) {
                peak = Math.max(peak, dailyReturn);
                maxDrawdown = Math.min(maxDrawdown, peak - dailyReturn);
            }

            return maxDrawdown;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Calmar Ratio calculator");
        job.setJarByClass(CalmarRatioCalculator.class);
        job.setMapperClass(CalmarMapper.class);
        job.setReducerClass(CalmarReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

