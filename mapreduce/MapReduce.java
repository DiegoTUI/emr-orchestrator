import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapReduce {

    public static class Map extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) {
            String[] items = value.toString().split("\\|");
            SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
            if(items.length == 15) {
                String requestDateString = new String(items[0].substring(0,10));
                String destination = new String(this.sortDestinations(items[4]));
                String entryDateString = new String(items[6]);
                String numberHotelsReturned = new String(items[13]);
                int daysDiff = -1;
                // parse dates
                try {
                    long diff = formatter.parse(entryDateString).getTime() - formatter.parse(requestDateString).getTime();
                    daysDiff = (int)TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
                }
                catch (ParseException e) {
                    e.printStackTrace();
                }
                // print result
                Text result = new Text(requestDateString + "|" + destination + "|" + daysDiff + "|" + numberHotelsReturned);
                try {
                    context.write(result, null);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private String sortDestinations(String destinations) {
            String[] elements = destinations.split("#");
            if (elements.length == 1) {
                return destinations;
            }
            Arrays.sort(elements);
            return Arrays.toString(elements).replace(", ", "#").replaceAll("[\\[\\]]", "");
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Mapper_Only_Job");
     
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(Map.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
            
        // Sets reducer tasks to 0
        job.setNumReduceTasks(0);
     
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
     
        boolean result = job.waitForCompletion(true);
     
        System.exit(result ? 0 : 1);
    }
}