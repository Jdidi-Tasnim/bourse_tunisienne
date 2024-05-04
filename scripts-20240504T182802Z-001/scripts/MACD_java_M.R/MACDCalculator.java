import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MACDCalculator {

    public static class MACDMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

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

    public static class MACDReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private static final int SHORT_PERIOD = 12; // Short-term moving average period
        private static final int LONG_PERIOD = 26; // Long-term moving average period

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            List<Double> priceList = new ArrayList<>();
            for (DoubleWritable value : values) {
                priceList.add(value.get());
            }

            if (priceList.size() >= LONG_PERIOD) {
                double shortEMA = calculateEMA(priceList, SHORT_PERIOD);
                double longEMA = calculateEMA(priceList, LONG_PERIOD);
                double macd = shortEMA - longEMA;
                context.write(key, new DoubleWritable(macd));
            } else {
                context.write(key, new DoubleWritable(-1.0)); // Indicate insufficient data for MACD calculation
            }
        }

        private double calculateEMA(List<Double> prices, int period) {
            double ema = 0.0;
            double multiplier = 2.0 / (period + 1);
            for (int i = 0; i < period; i++) {
                ema += prices.get(i);
            }
            ema /= period;
            for (int i = period; i < prices.size(); i++) {
                ema = (prices.get(i) - ema) * multiplier + ema;
            }
            return ema;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MACD calculator");
        job.setJarByClass(MACDCalculator.class);
        job.setMapperClass(MACDMapper.class);
        job.setReducerClass(MACDReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
