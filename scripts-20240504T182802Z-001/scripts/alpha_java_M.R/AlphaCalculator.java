import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AlphaCalculator {

    public static class AlphaMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text companyName = new Text();
        private DoubleWritable dailyReturn = new DoubleWritable();

        // Initialize the expected returns map with your model's predictions
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // You can initialize any required resources here
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            // Assuming the structure of your input is: SEANCE, GROUPE, CODE, VALEUR, OUVERTURE, CLOTURE, PLUS_BAS, PLUS_HAUT, QUANTITE_NEGOCIEE, NB_TRANSACTION, CAPITAUX, IND_RES
            if (tokens.length >= 6) { // Ensure there are at least 6 columns in the input
                String companyNameStr = tokens[3]; // Assuming the company name is in the fourth column
                String openStr = tokens[4];
                String closeStr = tokens[5];

                // Check if both opening and closing prices are valid (not "\N")
                if (!openStr.equals("\\N") && !closeStr.equals("\\N")) {
                    double open = Double.parseDouble(openStr); // Assuming opening price is in the fifth column
                    double close = Double.parseDouble(closeStr); // Assuming closing price is in the sixth column
                    double dailyReturnValue = (close - open) / open; // Daily return calculation

                    companyName.set(companyNameStr);
                    dailyReturn.set(dailyReturnValue);
                    context.write(companyName, dailyReturn);
                }
            }
        }
    }

    public static class AlphaReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sumAlpha = 0.0;
            int count = 0;

            // Compute the sum of alpha values
            for (DoubleWritable value : values) {
                sumAlpha += value.get();
                count++;
            }

            // Calculate the average alpha
            double averageAlpha = sumAlpha / count;

            context.write(key, new DoubleWritable(averageAlpha));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Alpha calculator");
        job.setJarByClass(AlphaCalculator.class);
        job.setMapperClass(AlphaMapper.class);
        job.setReducerClass(AlphaReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
