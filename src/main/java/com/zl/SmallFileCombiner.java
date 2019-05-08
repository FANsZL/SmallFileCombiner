package com.zl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SmallFileCombiner extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
//
//        System.setProperty("HADOOP_USER_NAME", "hadoop");

        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf);

        FileStatus[] status = fs.globStatus(new Path(args[0]), path -> {
            boolean accepted = false;
            try {
                FileStatus stat = fs.getFileStatus(path);
                accepted = stat.getLen() < stat.getBlockSize();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return accepted;
        });

        Path[] listedPaths = FileUtil.stat2Paths(status);

        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);

        job.setJarByClass(SmallFileCombiner.class);

        job.setMapperClass(SmallFileCombinerMapper.class);
        job.setReducerClass(SmallFileCombinerReducer.class);

        job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //MultipleOutputs.addNamedOutput(job, "20180718", TextOutputFormat.class, Text.class, Text.class);



        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);


        job.setInputFormatClass(ComFileInputFormat.class);
        job.setOutputFormatClass(MyOutputFormatClass.class);

        FileInputFormat.setInputPaths(job, listedPaths);


        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        job.setNumReduceTasks(0);

        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static class SmallFileCombinerMapper extends Mapper<LongWritable, Text, Text, Text>{
        //private Text file = new Text();
        private MultipleOutputs mos;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            mos = new MultipleOutputs(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String fileName = context.getConfiguration().get("map.input.file.name");
            String str = fileName.toString();
            String file = str.substring(0,str.length()-2);
            //file.set(fileName);
            NullWritable v = NullWritable.get();
            //context.write(file, value);
            mos.write(value,v,file);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            CombineFileSplit inputSplit = (CombineFileSplit)context.getInputSplit();
            Configuration conf = context.getConfiguration();
            Path[] paths = inputSplit.getPaths();
            FileSystem fs = FileSystem.get(conf);
            for(Path path : paths){
                if(fs.exists(path))
                    Trash.moveToAppropriateTrash(fs,path.getParent(), conf);
            }
            mos.close();
        }
    }

    public static class SmallFileCombinerReducer extends Reducer<Text, Text, Text, Text>{
        private MultipleOutputs mos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String str = key.toString();
            String filename = str.substring(0,str.length()-2);
            NullWritable v = NullWritable.get();

            for(Text t : values){
                mos.write(v, t, filename);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static class MyOutputFormatClass extends TextOutputFormat<Text, Text>{
        public Path getDefaultWorkFile(TaskAttemptContext context,String extension) throws IOException{
            FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
            return new Path(committer.getWorkPath(),getOutputName(context)+extension);
        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SmallFileCombiner(), args);
        System.exit(exitCode);
    }
}

