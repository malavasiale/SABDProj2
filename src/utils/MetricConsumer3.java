package utils;

import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class MetricConsumer3  implements IMetricsConsumer {
    FileWriter throughput;
    FileWriter distance ;
    FileWriter partial ;
    FileWriter global ;
    @Override
    public void prepare(Map map, Object o, TopologyContext topologyContext, IErrorReporter iErrorReporter) {
        try {
            throughput = new FileWriter("/data/test/query3_throughput.txt",true);
            distance = new FileWriter("/data/test/query3_distance.txt",true);
            partial = new FileWriter("/data/test/query3_partial.txt",true);
            global = new FileWriter("/data/test/query3_global.txt",true);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handleDataPoints(IMetricsConsumer.TaskInfo taskInfo, Collection<DataPoint> collection) {

        for(IMetricsConsumer.DataPoint p : collection){
            if(p.name.equals("Throughput")){
                try {
                    throughput.write(p.value.toString()+"\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else if(p.name.equals("Latency-distance")){
                try {
                    distance.write(p.value+"\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }else if(p.name.equals("Latency-partial")){
                try {
                    partial.write(p.value+"\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }else if(p.name.equals("Latency-global")){
                try {
                    global.write(p.value+"\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }


            }
        }
        try {
            partial.flush();
            global.flush();
            distance.flush();
            throughput.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {

    }
}
