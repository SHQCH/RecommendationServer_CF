import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * raw data: netflix dataset
 * input data: movieID, userID, rating
 * output:
 */

public class ItemIndexedData {
    public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        /**
         *
         * input: movie, user, rating
         * output: key = movie, value=user:rating
         *
         * the default inputFormat is textInputFormat, so the key is offset of each row in the file, value = lineText,
         **/

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] val = value.toString().trim().split(",");
            Integer movie = Integer.parseInt(val[0]);
            String user = val[1];
            String rating = val[2];
            context.write(new IntWritable(movie), new Text(user + ":" + rating));

        }
    }

    public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        /** input: key = movie, value = {user : rating} list
         * output: movie /t user1:rating1, user2:rating2,.., % avg_rating % div_rating
         * NOTE: div = (ri-r_avg)^2  aggregate on ri of the same item
         * find the average rating of each movie
         * output: movie /t avg_rating
         **/

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();
            int sum = 0;
            int count = 0;
            Double div = 0.0;
            Double average = 0.0;
            for(Text value : values){
                String[] tokens = value.toString().trim().split(":");
                Integer rating = Integer.parseInt(tokens[1]);
                sum += rating;
                count++;
                div += (double)rating * rating;
                sb.append("," + value);
            }

            if(count!=0) average = (double)sum/count;
            div = div- count * average * average;

            DecimalFormat df = new DecimalFormat("#.0000");
            String divText = Double.valueOf(df.format(div)).toString();
            String averageText = Double.valueOf(df.format(average)).toString();

            sb.append("%" + averageText);
            sb.append("%" + divText);
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(DataDividerMapper.class);
        job.setReducerClass(DataDividerReducer.class);
        job.setJarByClass(UserIndexedData.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
