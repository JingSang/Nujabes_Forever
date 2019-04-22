package com.hankcs.hanlp.corpus.io;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.CRF.CRFSegment;
import com.hankcs.hanlp.seg.Segment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * Created by root on 2017/8/9.
 */
public class HadoopFileIoAdapter implements IIOAdapter {

    @Override
    public InputStream open(String path) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(path), conf);
        return fs.open(new Path(path));
    }

    @Override
    public OutputStream create(String path) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(path), conf);
        OutputStream out = fs.create(new Path(path));
        return out;
    }
}
