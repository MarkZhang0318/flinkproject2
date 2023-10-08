package com.bw.flink.project;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.common.protocol.types.Field;

import java.io.*;
import java.util.concurrent.TimeUnit;

public class FileSource implements SourceFunction<String> {
    private String filePath;
    private BufferedReader fileReader;

    public FileSource(String filePath) {
        this.filePath = filePath;
    }
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        fileReader = new BufferedReader(new FileReader(filePath));
        String content = null;
        while ((content = fileReader.readLine()) != null) {
            TimeUnit.SECONDS.sleep(2);

            ctx.collect(content);

        }
        if (fileReader.readLine() == null) {
            fileReader.close();
        }
    }

    @Override
    public void cancel() {
        try {
            if (fileReader.readLine() == null) {
                fileReader.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
