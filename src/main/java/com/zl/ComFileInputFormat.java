package com.zl;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ComFileInputFormat extends CombineFileInputFormat<LongWritable,Text> {

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        //向过滤池pools中添加过滤器。
        List<FileStatus> stats = listStatus(job);
        ArrayList<String> collections = new ArrayList<String>();
        for(FileStatus stat : stats){
            final String dir = stat.getPath().getParent().getName();
            if(collections.contains(dir.substring(0,dir.length()-2))){
                continue;
            }
            collections.add(dir.substring(0,dir.length()-2));
            PathFilter filter = new PathFilter() {
                public boolean accept(Path path) {
                    String file = path.getParent().getName();
                    return file.substring(0,file.length()-2).equals(dir.substring(0,dir.length()-2));
                }
            };
            createPool(filter);
        }
        return super.getSplits(job);
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        CombineFileSplit combineFileSplit = (CombineFileSplit) inputSplit;
        CombineFileRecordReader recordReader = new CombineFileRecordReader<>(combineFileSplit, taskAttemptContext, ComFileRecordReader.class);

        try {
            recordReader.initialize(combineFileSplit, taskAttemptContext);
        } catch (InterruptedException e) {
            new RuntimeException("Errow to initialize com.zl.ComFileRecordReader");
        }

        return  recordReader;
    }
}

