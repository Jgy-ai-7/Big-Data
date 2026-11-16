import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankMR {

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split("\t");
            if (parts.length < 3) return;
            String page = parts[0];
            double pr = Double.parseDouble(parts[1]);
            String links = parts[2];

            context.write(new Text(page), new Text("S:" + pr + "\t" + links));

            if (!links.isEmpty()) {
                String[] outs = links.split(",");
                double contrib = pr / outs.length;
                for (String o : outs) {
                    String t = o.trim();
                    if (!t.isEmpty()) {
                        context.write(new Text(t), new Text("V:" + contrib));
                    }
                }
            }
        }
    }

    public static class PRReducer extends Reducer<Text, Text, Text, Text> {
        private int n = 15;
        private boolean isLast = false;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            n = conf.getInt("node.count", 15);
            isLast = conf.getBoolean("is.last", false);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            String links = "";
            boolean hasSelf = false;

            for (Text v : values) {
                String s = v.toString();
                if (s.startsWith("S:")) {
                    hasSelf = true;
                    String rest = s.substring(2);
                    String[] p = rest.split("\t");
                    links = p.length > 1 ? p[1] : "";
                } else if (s.startsWith("V:")) {
                    sum += Double.parseDouble(s.substring(2));
                }
            }

            if (!hasSelf) return;

            double pr = 0.15 / n + 0.85 * sum;

            if (isLast) {
                context.write(new Text("(" + key.toString() + "," + String.format("%.10f", pr) + ")"), new Text(""));
            } else {
                context.write(key, new Text(String.format("%.10f", pr) + "\t" + links));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: PageRankMR <input> <outputBase> [iterations] [nodeCount]");
            System.exit(1);
        }
        String input = args[0];
        String outputBase = args[1];
        int iterations = args.length > 2 ? Integer.parseInt(args[2]) : 10;
        int n = args.length > 3 ? Integer.parseInt(args[3]) : 15;

        String current = input;
        for (int i = 1; i <= iterations; i++) {
            Configuration conf = new Configuration();
            conf.setInt("node.count", n);
            conf.setBoolean("is.last", i == iterations);

            Job job = Job.getInstance(conf, "pagerank_iter_" + i);
            job.setJarByClass(PageRankMR.class);
            job.setMapperClass(PRMapper.class);
            job.setReducerClass(PRReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(1);

            FileInputFormat.addInputPath(job, new Path(current));
            Path out = new Path(outputBase + "/iter" + i);
            FileOutputFormat.setOutputPath(job, out);

            if (!job.waitForCompletion(true)) {
                System.err.println("Iteration " + i + " failed");
                System.exit(1);
            }
            current = out.toString();
            System.out.println("Iteration " + i + " finished, output: " + current);
        }
        System.out.println("All done. Final result: " + outputBase + "/iter" + iterations + "/part-r-00000");
    }
}
