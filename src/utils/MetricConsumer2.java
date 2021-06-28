package utils;

import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class MetricConsumer2 implements IMetricsConsumer{
    FileWriter throughput;
    FileWriter sector ;
    FileWriter count ;
    FileWriter sum ;
    FileWriter rank;
    @Override
    public void prepare(Map map, Object o, TopologyContext topologyContext, IErrorReporter iErrorReporter) {
        try {
            throughput = new FileWriter("/data/test/query2_throughput.txt");
            sector = new FileWriter("/data/test/query2_sector.txt");
            count = new FileWriter("/data/test/query2_count.txt");
            sum = new FileWriter("/data/test/query2_sum.txt");
            rank = new FileWriter("/data/test/query2_rank.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handleDataPoints(IMetricsConsumer.TaskInfo taskInfo, Collection<IMetricsConsumer.DataPoint> collection) {

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
                    if(name.equals("sum")){
                        sum.write(value+"\n");
                    }else if(name.equals("count")){
                        count.write(value+"\n");
                    }else if(name.equals("sector")){
                        sector.write(value+"\n");
                    }else if(name.equals("rank")) {
                        rank.write(value + "\n");
                    }
                }catch (IOException e){
                    e.printStackTrace();
                }

            }

            //System.out.println(p.name+"  "+p.value.toString()+"\n");
        }
        try {
            sum.flush();
            count.flush();
            sector.flush();
            throughput.flush();
            rank.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {

    }
}

