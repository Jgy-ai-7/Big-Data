import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GrandChild {
    public static class Map extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("child")) return;
            String[] parts = line.split("\\s+");
            if (parts.length < 2) return;
            String child = parts[0];
            String parent = parts[1];
            context.write(new Text(parent), new Text("child:" + child));
            context.write(new Text(child), new Text("parent:" + parent));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private static Text outKey = new Text("grandchild");
        private static Text outVal = new Text("grandparent");
        private boolean first = true;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> children = new ArrayList<>();
            List<String> parents = new ArrayList<>();
            for (Text val : values) {
                String s = val.toString();
                if (s.startsWith("child:")) children.add(s.substring(6));
                else if (s.startsWith("parent:")) parents.add(s.substring(7));
            }
            if (first) {
                context.write(outKey, outVal);
                first = false;
            }
            for (String child : children) {
                for (String parent : parents) {
                    context.write(new Text(child), new Text(parent));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "grandchild");
        job.setJarByClass(GrandChild.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
