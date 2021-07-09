package utils;

import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/*
    Classe che funziona da consumatore esterno delle metriche prodotte
    dai vari bolt della topologia
*/
public class MetricConsumer1 implements IMetricsConsumer {
    FileWriter throughput;
    FileWriter sector ;
    FileWriter count ;
    FileWriter sum ;
    @Override
    public void prepare(Map map, Object o, TopologyContext topologyContext, IErrorReporter iErrorReporter) {
        // Preparo i file all' interno dei quali riversare le metriche
        try {
            throughput = new FileWriter("/data/test/query1_throughput.txt",true);
            sector = new FileWriter("/data/test/query1_sector.txt",true);
            count = new FileWriter("/data/test/query1_count.txt",true);
            sum = new FileWriter("/data/test/query1_sum.txt",true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> collection) {

        //Parsing delle tuple rappresentati le diverse metriche
        for(DataPoint p : collection){
                if(p.name.equals("Throughput")){
                    try {
                        throughput.write(p.value.toString()+"\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }else if(p.name.equals("Latency-sector")){
                    try {
                        sector.write(p.value+"\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }else if(p.name.equals(("Latency-sum"))){
                    try {
                        sum.write(p.value+"\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }else if(p.name.equals("Latency-count")){
                    try {
                        count.write(p.value+"\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
         }
        try {
            sum.flush();
            count.flush();
            sector.flush();
            throughput.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {

    }
}
