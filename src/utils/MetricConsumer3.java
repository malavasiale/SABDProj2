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
            throughput = new FileWriter("/data/test/query3_throughput.txt");
            distance = new FileWriter("/data/test/query3_distance.txt");
            partial = new FileWriter("/data/test/query3_partial.txt");
            global = new FileWriter("/data/test/query3_global.txt");

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
            }else if(p.name.equals("__execute-latency")){
                if(p.value.toString().equals("{}")){
                    continue;
                }
                String line =p.value.toString();
                line = line.replaceAll("\\{|\\}","");
                System.out.println(line+"\n");
                String[] line_split = line.split(":");
                String name = line_split[0];
                String[] value_split = line_split[1].split("=");
                String value = value_split[1];
                try{
                    if(name.equals("distance")){
                        distance.write(value+"\n");
                    }else if(name.equals("partial")){
                        partial.write(value+"\n");
                    }else if(name.equals("global")){
                        global.write(value+"\n");
                    }
                }catch (IOException e){
                    e.printStackTrace();
                }

            }

            //System.out.println(p.name+"  "+p.value.toString()+"\n");
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
