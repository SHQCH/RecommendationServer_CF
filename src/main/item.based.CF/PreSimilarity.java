import org.apache.hadoop.conf.Configuration;
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
import java.util.Map;
import java.util.HashMap;

/**
 * prepare data for similarity calculation
 * raw input from UserIndexedData value = movie1 \t movie2:S12(from user1)
 * output: key = movie1, value = {sim:movie2:S12, sim:movie3:S13}
 */
public class PreSimilarity {
    public static class PreSimMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        /**
         * user /t  value = movie1 \t movie2:S12(from user1)
         * output: key = movie1, value= movie2:S12
         **/

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] keyval = value.toString().trim().split("\\t");
            Integer movie1 = Integer.parseInt(keyval[0]);
            context.write(new IntWritable(movie1), new Text(keyval[1]));
        }
    }

    public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        /** input: key = movie, value = {movie1, value= movie2:S12} list,
         * output: key = movie /t  value = sum of S12 under the same movie
         **/

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Map<Integer, Double> map = new HashMap<Integer, Double>();

            for(Text val : values) {
                String[] valStrings = val.toString().trim().split(":");
                Integer movieID = Integer.parseInt(valStrings[0]);
                Double rating = Double.parseDouble(valStrings[1]);
                if(map.get(movieID)!=null) {
                    map.put(movieID, map.get(movieID)+rating);
                }
                else
                    map.put(movieID, rating);
            }

            // stored all sim value of movie_key & movieID into the hashMap
            StringBuilder sb = new StringBuilder();
            for(Map.Entry<Integer, Double> mr : map.entrySet()) {
                sb.append("sim:"+mr.getKey().toString()+":"+mr.getValue().toString());
                sb.append(",");
                context.write(key, new Text(sb.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(ItemIndexedData.DataDividerMapper.class);
        job.setReducerClass(ItemIndexedData.DataDividerReducer.class);
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
