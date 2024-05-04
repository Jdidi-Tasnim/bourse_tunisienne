import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ExponentialMovingAverage {

    public static class EmaMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text companyName = new Text();
        private DoubleWritable closingPrice = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            // Assuming the structure of your input is: DATE, GROUPE, CODE, VALEUR, OUVERTURE, CLOTURE, PLUS_BAS, PLUS_HAUT, QUANTITE_NEGOCIEE, NB_TRANSACTION, CAPITAUX, IND_RES
            String companyNameStr = tokens[3]; // Assuming the company name is in the fourth column
            double close = Double.parseDouble(tokens[5]); // Assuming closing price is in the sixth column
            companyName.set(companyNameStr);
            closingPrice.set(close);
            context.write(companyName, closingPrice);
        }
    }

    public static class EmaReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable emaValue = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double alpha = 0.2; // Exponential smoothing factor (adjust as needed)
            double prevEma = 0.0;
            boolean firstValue = true;

            for (DoubleWritable value : values) {
                double close = value.get();
                if (firstValue) {
                    prevEma = close;
                    firstValue = false;
                } else {
                    prevEma = alpha * close + (1 - alpha) * prevEma;
                }
            }

            emaValue.set(prevEma);
            context.write(key, emaValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "exponential moving average");
        job.setJarByClass(ExponentialMovingAverage.class);
        job.setMapperClass(EmaMapper.class);
        job.setReducerClass(EmaReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

