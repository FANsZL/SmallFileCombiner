package com.zl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;


//在其内部封装一个LineRecordReader,用来从split中读取数据。

public class ComFileRecordReader extends RecordReader<LongWritable, Text> {


    protected CombineFileSplit comFileSplit;
    protected LineRecordReader lineRecordReader = new LineRecordReader();
    protected Path[] paths;
    protected long totLength;
    protected int curIndex;
    protected float curProgress;
    protected LongWritable curKey;
    protected Text curVal;
    private boolean fileNotFound = false;

    public ComFileRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException, InterruptedException {
        super();
        curIndex = index;
        comFileSplit = split;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        fileNotFound = false;
        comFileSplit = (CombineFileSplit) inputSplit;
        FileSplit fileSplit = new FileSplit(comFileSplit.getPath(curIndex),comFileSplit.getOffset(curIndex),comFileSplit.getLength(curIndex),comFileSplit.getLocations());
        try {
            lineRecordReader.initialize(fileSplit, taskAttemptContext);
        }catch (IOException e){
            fileNotFound = true;
            System.out.println(e.getMessage());
        }

        paths = comFileSplit.getPaths();
        totLength = paths.length;
        taskAttemptContext.getConfiguration().set("map.input.file.name", comFileSplit.getPath(curIndex).getParent().getName());
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!fileNotFound && curIndex >= 0 && curIndex < totLength) {
            return lineRecordReader.nextKeyValue();
        }
        return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        curKey = lineRecordReader.getCurrentKey();
        return curKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        curVal = lineRecordReader.getCurrentValue();
        return curVal;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if(curIndex >= 0 && curIndex < totLength){
            curProgress = (float) curIndex / totLength;
            return curProgress;
        }
        return curProgress;
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }


}

