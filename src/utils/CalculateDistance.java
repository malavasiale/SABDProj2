package utils;


public class CalculateDistance {

    public static Double euclideanDistance(Double prev_lat, Double prev_lon, Double last_lat, Double last_lon){
        double earthRadius = 6371000; //meters
        double dLat = Math.toRadians(prev_lat-last_lat);
        double dLng = Math.toRadians(prev_lon-last_lon);
        double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                Math.cos(Math.toRadians(prev_lat)) * Math.cos(Math.toRadians(last_lat)) *
                        Math.sin(dLng/2) * Math.sin(dLng/2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        Double dist = (earthRadius * c);

        return dist;
    }

}
