package utils;

import com.rabbitmq.client.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import query1.Query1Topology;
import query2.Query2Topology;
import query3.Query3Topology;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.concurrent.TimeoutException;

// RabbitMQ manager

public class RabbitMQManager {

    private String host;
    private String username;
    private String password;
    private ConnectionFactory factory;
    private Connection connection;
    public static PrintWriter writer;
    private String defaultQueue;

    public RabbitMQManager(String host,String username, String password, String queue,String path){
        super();
        this.host = host;
        this.username = username;
        this.password = password;

        this.factory = null;
        this.connection = null;
        this.defaultQueue = queue;

        this.initialize();
        this.initializeQueue(defaultQueue);
        Configuration conf = new Configuration();
        conf.addResource(new Path("/data/Hadoop/core-site.xml"));
        conf.addResource(new Path("/data/Hadoop/hdfs-site.xml"));
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );

        FileSystem fs = null;
        FSDataOutputStream outputStream;

        try {
            if(RabbitMQConsumer.filename.equals("Query1.csv")){
                fs = FileSystem.get(URI.create("hdfs://localhost:9000/results/"+path),conf);
                outputStream = fs.create(new Path("/results/"+path));
            }else if(RabbitMQConsumer.filename.equals("Query2.csv")){
                fs = FileSystem.get(URI.create("hdfs://localhost:9000/results/"+path),conf);
                outputStream = fs.create(new Path("/results/"+path));
            }else{
                fs = FileSystem.get(URI.create("hdfs://localhost:9000/results/"+path),conf);
                outputStream = fs.create(new Path("/results/"+path));
            }
            writer = new PrintWriter(outputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public RabbitMQManager(String host, String username, String password, String queue) {
        super();
        this.host = host;
        this.username = username;
        this.password = password;

        this.factory = null;
        this.connection = null;
        this.defaultQueue = queue;

        this.initialize();
        this.initializeQueue(defaultQueue);

    }

    private void initializeQueue(String queue){

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setUsername(username);
        factory.setPassword(password);
        Connection connection;
        try {
            connection = factory.newConnection();
            Channel channel = connection.createChannel();

            boolean durable = false;
            boolean exclusive = false;
            boolean autoDelete = false;

            channel.queueDeclare(queue, durable, exclusive, autoDelete, null);

            channel.close();
            connection.close();

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }

    }

    private void initialize(){

        factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setUsername(username);
        factory.setPassword(password);
        try {

            connection = factory.newConnection();

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    public void terminate(){

        if (connection != null && connection.isOpen()){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private boolean reopenConnectionIfNeeded(){

        try {

            if (connection == null){
                connection = factory.newConnection();
                return true;
            }

            if (!connection.isOpen()){
                connection = factory.newConnection();
            }

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            return false;
        }

        return true;

    }

    public boolean send(String message){
        return this.send(defaultQueue, message);
    }

    public boolean send(String queue, String message){

        try {

            reopenConnectionIfNeeded();
            Channel channel = connection.createChannel();
            channel.basicPublish("", queue, null, message.getBytes());
            channel.close();

            return true;

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }

        return false;

    }

    public boolean createDetachedReader(String queue) {



        try {

            reopenConnectionIfNeeded();

            Channel channel = connection.createChannel();

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                           byte[] body)
                        throws IOException {
                    String message = new String(body, "UTF-8");
                    System.out.println(message);
                    writer.append(message+"\n");
                    writer.flush();

                }
            };
            channel.basicConsume(queue, true, consumer);

            return true;

        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;

    }

}