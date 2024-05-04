import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RSICalculator {

    public static class RSIMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

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

    public static class RSIReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private static final int PERIOD = 14; // RSI calculation period

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            List<Double> priceList = new ArrayList<>();
            for (DoubleWritable value : values) {
                priceList.add(value.get());
            }

            if (priceList.size() >= PERIOD) {
                double avgGain = 0.0;
                double avgLoss = 0.0;

                for (int i = 1; i < PERIOD; i++) {
                    double priceDiff = priceList.get(i) - priceList.get(i - 1);
                    if (priceDiff >= 0) {
                        avgGain += priceDiff;
                    } else {
                        avgLoss -= priceDiff;
                    }
                }

                avgGain /= PERIOD;
                avgLoss /= PERIOD;

                double rs = avgGain / avgLoss;
                double rsi = 100 - (100 / (1 + rs));

                context.write(key, new DoubleWritable(rsi));
            } else {
                context.write(key, new DoubleWritable(-1.0)); // Indicate insufficient data for RSI calculation
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "RSI calculator");
        job.setJarByClass(RSICalculator.class);
        job.setMapperClass(RSIMapper.class);
        job.setReducerClass(RSIReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
