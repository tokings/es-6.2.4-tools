package cluster.dbscan;

import java.util.ArrayList;

/**
 * Created by Jason on 2016/4/17.
 */
public class Client {


    public static void main(String[] args) {
        //ArrayList<Point> points = Data.generateSinData(200);
        //DBScan dbScan = new DBScan(0.6,4);
        ArrayList<Point> points = Data.generateSpecialData(250);
        DBScan dbScan = new DBScan(300000,3);
        dbScan.process(points);
        for (Point p:points) {
            System.out.println(p);
        }
        Data.writeData(points,"data.txt");
    }

}
