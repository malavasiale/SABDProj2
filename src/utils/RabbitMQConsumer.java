package utils;

public class RabbitMQConsumer {

    /**
     * Classe che permette di leggere i dati da una coda RabbitMQ
     * @param args
     * @throws InterruptedException
     */

    public static void main(String[] args) throws InterruptedException {

        String rabbitMQ = "localhost";
        String rabbitMQUsername = "rabbitmq";
        String rabbitMQPassword = "rabbitmq";
        String rabbitMQQueue = args[0];

        RabbitMQManager rmq = new RabbitMQManager(rabbitMQ, rabbitMQUsername, rabbitMQPassword, rabbitMQQueue);

        System.out.println(rmq.createDetachedReader(rabbitMQQueue));	//RabbitMQ reader

        while(true){
            Thread.sleep(500);
        }

    }

}
