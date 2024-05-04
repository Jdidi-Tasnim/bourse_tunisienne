import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class VolatilityCalculator {

    public static class VolatilityMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text companyName = new Text();
        private DoubleWritable closingPrice = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            // Assuming the structure of your input is: DATE, GROUPE, CODE, VALEUR, OUVERTURE, CLOTURE, PLUS_BAS, PLUS_HAUT, QUANTITE_NEGOCIEE, NB_TRANSACTION, CAPITAUX, IND_RES
            String companyNameStr = tokens[3]; // Assuming the company name is in the fourth column
            double closingPriceValue = Double.parseDouble(tokens[5]); // Assuming closing price is in the sixth column
            companyName.set(companyNameStr);
            closingPrice.set(closingPriceValue);
            context.write(companyName, closingPrice);
        }
    }

    public static class VolatilityReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable volatility = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            double sumOfSquares = 0.0;
            int count = 0;

            for (DoubleWritable value : values) {
                double price = value.get();
                sum += price;
                sumOfSquares += price * price;
                count++;
            }

            double mean = sum / count;
            double variance = (sumOfSquares / count) - (mean * mean);
            double standardDeviation = Math.sqrt(variance);
            volatility.set(standardDeviation);
            context.write(key, volatility);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "volatility calculator");
        job.setJarByClass(VolatilityCalculator.class);
        job.setMapperClass(VolatilityMapper.class);
        job.setReducerClass(VolatilityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
