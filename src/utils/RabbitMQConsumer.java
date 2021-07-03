package utils;

public class RabbitMQConsumer {
    public static String filename;

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
        //TODO aggiungere nome file tramite args[1] e passarlo al costruttore al posto di truegi

        if(rabbitMQQueue.equals("query1")){
            filename="Query1.csv";
            System.out.println("ts,id_cella,ship_t35,avg_t35,ship_t60,avg_t60,ship_t70,avg_t70,ship_to,avg_to");
        }else if(rabbitMQQueue.equals("query2")){
            filename="Query2.csv";
            System.out.println("ts,sea,slot_a,rank_a,slot_b,rank_b");
        }else if(rabbitMQQueue.equals("query3")){
            filename="Query3.csv";
            System.out.println("ts,trip_1,rating_1,trip_2,rating_2,trip_3,rating_3,trip_4,rating_4,trip_5,rating_5");
        }

        RabbitMQManager rmq = new RabbitMQManager(rabbitMQ, rabbitMQUsername, rabbitMQPassword, rabbitMQQueue,args[1]);

        rmq.createDetachedReader(rabbitMQQueue);	//RabbitMQ reader
        while(true){
            Thread.sleep(500);
        }

    }

}
