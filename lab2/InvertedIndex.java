import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

    public static class IndexMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text docName = new Text();
        private Set<String> stopWords = new HashSet<String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String stopPath = conf.get("stopwords");
            if (stopPath != null) {
                Path p = new Path(stopPath);
                FileSystem fs = FileSystem.get(conf);
                if (fs.exists(p)) {
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
                    String line;
                    while ((line = br.readLine()) != null) {
                        stopWords.add(line.trim().toLowerCase());
                    }
                    br.close();
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            String fileName = split.getPath().getName();
            docName.set(fileName);

            String line = value.toString().toLowerCase();
            String[] words = line.split("[^a-z]+");
            for (String w : words) {
                if (w.isEmpty()) continue;
                if (stopWords.contains(w)) continue;
                word.set(w);
                context.write(word, docName);
            }
        }
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> docCount = new HashMap<String, Integer>();
            int total = 0;
            for (Text val : values) {
                String doc = val.toString();
                Integer c = docCount.get(doc);
                if (c == null) c = 0;
                docCount.put(doc, c + 1);
                total++;
            }
            List<String> docs = new ArrayList<String>(docCount.keySet());
            Collections.sort(docs);
            StringBuilder sb = new StringBuilder();
            for (String doc : docs) {
                sb.append("<").append(doc).append(",").append(docCount.get(doc)).append(">;");
            }
            sb.append("<total,").append(total).append(">.");
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length > 2) {
            conf.set("stopwords", args[2]);
        }
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
