package utils;

public class ConvertToSector {

    static final  Double minLat = 32.0;
    static final  Double maxLat = 45.0;
    static final Double minLon = -6.0;
    static final Double maxLon = 37.0;

    static final String[] latID = {"A","B","C","D","F","G","H","I","J"};


    public static String convertPointToSector(Double lat, Double lon){
        if(lat > maxLat || lon > maxLon || lat < minLat || lon < minLon){
            return "Punto fuori range!";
        }

        Integer number_lat = (int)Math.round((lat - minLat)/1.3);
        Integer number_lon = (int)Math.round((lon - minLon)/1.075);

        String id_lat = latID[number_lat];
        String id_lon = number_lon.toString();

        return id_lat+id_lon;

    }

    public static boolean isOccidental(String id){
        if(id.charAt(0) =='A' ||id.charAt(0) =='B' || id.charAt(0) =='C'){
            return false;
        }else if (Integer.parseInt(id.substring(1)) > 17){
            return false;
        }else{
            return true;
        }
    }
}
