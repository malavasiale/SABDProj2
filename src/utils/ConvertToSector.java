package utils;

public class ConvertToSector {

    static final  Double minLat = 32.0;
    static final  Double maxLat = 45.0;
    static final Double minLon = -6.0;
    static final Double maxLon = 37.0;

    static final String[] latID = {"A","B","C","D","E","F","G","H","I","J"};

    /**
     * Metodo per individuare in che settore si trova una nave
     * @param lat
     * @param lon
     * @return String
     */
    public static String convertPointToSector(Double lat, Double lon){
        if(lat > maxLat || lon > maxLon || lat < minLat || lon < minLon){
            return "Coordinate non valide!";
        }

        double number_lat = ((lat - minLat)/1.3);
        int integer_lat = (int)number_lat;
        double number_lon = ((lon - minLon)/1.075);
        int integer_lon = (int)number_lon+1;



        String id_lat = latID[integer_lat];

        String id_lon = Integer.toString(integer_lon);

        return id_lat+id_lon;

    }

    /**
     * Metodo per verificare se un settore è presente nel Mar Mediterraneo Occidentale
     * @param id
     * @return Boolean
     */
    public static boolean isOccidental(String id){
        if (Integer.parseInt(id.substring(1)) > 17){
            return false;
        }else{
            return true;
        }
    }

    /**
     * Metodo per identificare il tipo di nave dato il suo id
     * @param type_number
     * @return String
     */
    public static String shipType(String type_number){
        Integer number = Integer.parseInt(type_number);
        if(number.equals(35)){
            return "militare";
        }else if (number >= 60 && number <= 69){
            return "passeggeri";
        }else if (number >= 70 && number <= 79){
            return "cargo";
        }else{
            return "other";
        }
    }

    /**
     * Metodo per verificare se un orario appartiene alla prima fascia 00:00 - 11:59 oppure alla seconda
     * 12:00 - 23:59
     * @param data
     * @return
     */
    public static String convertOrarioToFascia(String data) {
        String[] data_splitted = data.split(" ");
        String orario = data_splitted[1];
        String[] ora_string = orario.split(":");
        Integer ora = Integer.parseInt(ora_string[0]);
        if(ora<12){
            return "prima";
        }else{
            return "seconda";
        }
    }
}
