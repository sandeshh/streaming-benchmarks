package com.example;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by sandesh on 2/24/16.
 */
public class EventGenerator extends BaseOperator implements InputOperator {

    private ArrayList<String> ad_id = new ArrayList() ;
    private static String pageID = UUID.randomUUID().toString();
    private static String userID = UUID.randomUUID().toString();
    private static final String[] eventTypes = new String[]{"view", "click", "purchase"};
    private String mappingFile = "/user/sandesh/test.txt";

    public String getMappingFile() {
        return mappingFile;
    }

    public void setMappingFile(String mappingFile) {
        this.mappingFile = mappingFile;
    }

    public final transient DefaultOutputPort<String> out = new DefaultOutputPort<String>();

    public void setup(Context.OperatorContext context) {
        try {

            Path filePath = new Path(mappingFile);
            Configuration configuration = new Configuration();
            FileSystem fs;
            fs = FileSystem.newInstance(filePath.toUri(), configuration);
            FSDataInputStream inputStream = fs.open(filePath);
            BufferedReader bufferedReader;

            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            while ((line = bufferedReader.readLine()) != null) {

                String mapping[] = line.split("\\s+");
                ad_id.add(mapping[1]);
            }
        } catch (Exception e) {

        }
    }

    public String generateElement() {

        StringBuilder sb = new StringBuilder();

        sb.setLength(0);
        sb.append("{\"user_id\":\"");
        sb.append(pageID);
        sb.append("\",\"page_id\":\"");
        sb.append(userID);
        sb.append("\",\"ad_id\":\"");
        sb.append(ad_id.get(ThreadLocalRandom.current().nextInt(ad_id.size())));
        sb.append("\",\"ad_type\":\"");
        sb.append("banner78"); // value is immediately discarded. The original generator would put a string with 38/5 = 7.6 chars. We put 8.
        sb.append("\",\"event_type\":\"");
        sb.append(eventTypes[ThreadLocalRandom.current().nextInt(eventTypes.length)]);
        sb.append("\",\"event_time\":\"");
        sb.append(System.currentTimeMillis());
        sb.append("\",\"ip_address\":\"1.2.3.4\"}");

        return sb.toString();
    }

    @Override
    public void emitTuples() {
        out.emit( generateElement() ) ;
    }
}
