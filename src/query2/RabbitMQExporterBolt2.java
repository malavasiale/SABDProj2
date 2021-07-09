package query2;

import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import utils.RabbitMQManager;

import java.io.FileWriter;
import java.util.Map;

/*
   Esporta i dati in formato string su RabbitMQ
 */

public class RabbitMQExporterBolt2 extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    private RabbitMQManager rabbitmq;
    private String rabbitMqHost;
    private String rabbitMqUsername;
    private String rabbitMqPassword;
    private String defaultQueue;
    private CountMetric metric;

   /*
      Inizializzo RammbitMQManager
   */
    public RabbitMQExporterBolt2(String rabbitMqHost, String rabbitMqUsername, String rabbitMqPassword,
                                 String defaultQueue) {
        super();

        this.rabbitMqHost = rabbitMqHost;
        this.rabbitMqUsername = rabbitMqUsername;
        this.rabbitMqPassword = rabbitMqPassword;
        this.defaultQueue = defaultQueue;
    }
    
    /*
      Inizializzo metrica CountMetric() personalizzata
    */
    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.metric = new CountMetric();

        topologyContext.registerMetric("Throughput",metric,30);
        this.collector=outputCollector;
        this.rabbitmq = new RabbitMQManager(rabbitMqHost, rabbitMqUsername, rabbitMqPassword, defaultQueue);

    }

    @Override
    public void execute(Tuple tuple) {
        // Incremento di uno il contatore della metrica
        metric.incr();
        String output = tuple.getString(0);
        // Mando la tupla alla coda RabbitMQ
        rabbitmq.send("query2",output);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }



}
