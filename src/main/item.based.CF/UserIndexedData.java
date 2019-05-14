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
import java.util.ArrayList;
import java.util.List;

/**
 * input = data from ItemIndexedData
 *  raw data: value = movie /t user1:rating1, user2:rating2,.., % avg % div
 *  output: key = movie1, output = movie2: (r1-avg1)*(r2-avg2)/div1/div2
 */
public class UserIndexedData {
    public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {


        /**
         *  input: movie /t user1:rating1, user2:rating2,.., % avg_rating % div_rating
         *   output: user /t movie1:rating : avg : div
         */

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] keyValue = value.toString().trim().split("/t");
            String movie = keyValue[0];
            String[] val = keyValue[1].split("%");

            String[] userRatingPairs = val[0].trim().split(",");
            String avg = val[1];
            String div = val[2];

            for(String  userRating : userRatingPairs) {
                String[] pair = userRating.split(":");
                Integer user = Integer.parseInt(pair[0]);
                String rating = pair[1];
                StringBuilder sb = new StringBuilder(movie);
                sb.append(":" + rating);
                sb.append(":" + avg);
                sb.append((":" + div));
                context.write(new IntWritable(user), new Text(sb.toString()));
            }
        }
    }


    public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        /**
         * input: key = user, value = {movie : rating : avg : div} list
         * output: key = movie1 value = movie2: (r1-avg1)*(r2-avg2)/div1/div2,
         *         key = movie2 value = movie1: (r1-avg1)*(r2-avg2)/div1/div2,
         *
         * assuming the rating is normalized across users, that is, avg. rating over a single user = 0, for all users
        **/
         public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();
            List<String> ls = new ArrayList<String>();
            for(Text e : values) ls.add(e.toString());
            for(int i=0; i<ls.size(); i++) {
                String[] str1 = ls.get(i).split(":");
                Integer movie1 = Integer.parseInt(str1[0]);
                Double avg1 = Double.parseDouble(str1[2]);
                Double div1 = Double.parseDouble(str1[3]);
                Double rating1 = Integer.parseInt(str1[1])-avg1;

                for(int j=i+1; j<ls.size(); j++) {
                    String[] str2 = ls.get(j).split(":");
                    Integer movie2= Integer.parseInt(str2[0]);
                    Double avg2 = Double.parseDouble(str2[2]);
                    Double div2 = Double.parseDouble(str2[3]);
                    Double rating2 = Integer.parseInt(str2[1])-avg2;
                    Double product = rating1 * rating2 / (div1*div2);
                    DecimalFormat df = new DecimalFormat("#.0000");
                    String productTxt = Double.valueOf(df.format(product)).toString();

                    context.write(new IntWritable(movie1), new Text(movie2+":"+productTxt));
                    context.write(new IntWritable(movie2), new Text(movie1+":"+productTxt));

                }
            }

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

