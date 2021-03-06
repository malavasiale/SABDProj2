package query1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;
import utils.MetricConsumer1;

public class Query1Topology {

    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();
        
        /*
        * Costruisco la topologia per la query1
        */
        builder.setSpout("source",new ReaderCSVSpout1(),1);

        /*
        * Tramite TumblingWindow raggurppo i dati ogni 30 minuti
        */
        builder.setBolt("sector",new SectorConverterBolt1().withTimestampExtractor(new TimestampExtractor() {
            @Override
            public long extractTimestamp(Tuple tuple) {
                return tuple.getLong(0);
            }
        }).withTumblingWindow((BaseWindowedBolt.Duration.minutes(30))),1)
                .shuffleGrouping("source");

        builder.setBolt("count",new CountBolt1(),1)
                .shuffleGrouping("sector");

        builder.setBolt("sum",new SumBolt1(args[0]),3)
                .fieldsGrouping("count", new Fields("sector_id"));

        builder.setBolt("exporter",
                new RabbitMQExporterBolt1(
                        "localhost","rabbitmq" ,
                        "rabbitmq", "query1"),
                3)
                .shuffleGrouping("sum");

        Config conf = new Config();
        conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS,false);
        conf.put(Config.TOPOLOGY_DEBUG,false);
        
        /*
        * Configuro il bolt per consumare le metriche e ogni quanto tempo viene eseguito
        */
        conf.registerMetricsConsumer(MetricConsumer1.class,1);
        conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS,15);
        conf.setMaxTaskParallelism(3);
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("query1", conf, builder.createTopology());
    }
}
