import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * two input: (1) from output of PreSimilarity, movie1 /t sim:movie2:s12, sim:movie3:s13,...
 *            (2) from output of ItemIndexedData,   movie /t user1:rating1, user2:rating2,.., % avg_rating % div_rating
 * Output:  key = user1:movie2
 *          value = rating
 */
public class Multiplication {
    public static class SimMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input: value = movie1 /t sim:movie2:s12, sim:movie3:s13,...
            // output: key = movie1, value = sim:movie2:S12, sim:movie3:s13,...

            String[] tokens = value.toString().trim().split("\\t");
            Integer movie1 = Integer.parseInt(tokens[0]);

            context.write(new IntWritable(movie1), new Text(tokens[1]));
        }

    }

    public static class RateMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        // input:  value = movie/t user1:rating1, user2:rating2,.., % avg_rating % div_rating
        // output:  key=movie, value = user1:......

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().trim().split("\\t");
            int movie = Integer.parseInt(tokens[0]);
            context.write(new IntWritable(movie), new Text(tokens[1]));
        }
    }

    public static class SimJoinRatingRed extends Reducer<IntWritable, Text, Text, DoubleWritable> {

        /**
         * input: key= movie1, value = {sim:movie2:s12, sim:movie3:s13, user1:r1, user2:r2}
         * output: key=user1_movie2, value = r1*sim12
         *  the value is the sim vector of movie1, and rating vector of movie1
         *  the reduce is to find the cross product of these two vectors,
         *  i
         */

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            double threshold = 0.1;

            List<String> userRatList = new ArrayList<String>();
            List<String> simList = new ArrayList<String>();

            for(Text val : values) {
                String[] tokens = val.toString().trim().split("\\t");
                for(String token : tokens) {
                    String[] temp = token.split(":");
                    if(temp[0].equals("sim")) {
                        simList.add(token);
                    }
                    else userRatList.add(token);
                }
            }

            for(String sim : simList) {
                String[] simStrings = sim.split(":");
                String movie = simStrings[1];
                Double simVal = Double.parseDouble(simStrings[2]);
                for(String rat : userRatList) {
                    String[] ratStrings = rat.split(":");
                    String user = ratStrings[0];
                    Double rating = Double.parseDouble(ratStrings[1]);
                    context.write(new Text(user+":"+movie), new DoubleWritable(simVal*rating));
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(Multiplication.class);
        job.setReducerClass(SimJoinRatingRed.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SimMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RateMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }

}
