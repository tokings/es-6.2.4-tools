package cluster.dbscan;

import geo.calculate.Ellipsoid;
import geo.calculate.GEODistanceCaculator;

public class Point {
    private double x;
    private double y;
    private boolean isVisit;
    private int cluster;
    private boolean isNoised;

    public Point(double x,double y) {
        this.x = x;
        this.y = y;
        this.isVisit = false;
        this.cluster = 0;
        this.isNoised = false;
    }

    public double getDistance(Point point) {
//    	欧式距离
        return Math.sqrt((x-point.x)*(x-point.x)+(y-point.y)*(y-point.y));
//    	geo球面距离
//    	return GEODistanceCaculator.distanceOfTwoGEOPoints(Ellipsoid.Sphere.getSemiMajorAxis(), this.x, this.y, point.x, point.y);
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getX() {
        return x;
    }

    public void setY(double y) {
        this.y = y;
    }

    public double getY() {
        return y;
    }

    public void setVisit(boolean isVisit) {
        this.isVisit = isVisit;
    }

    public boolean getVisit() {
        return isVisit;
    }

    public int getCluster() {
        return cluster;
    }

    public void setNoised(boolean isNoised) {
        this.isNoised = isNoised;
    }

    public void setCluster(int cluster) {
        this.cluster = cluster;
    }

    public boolean getNoised() {
        return this.isNoised;
    }

    @Override
    public String toString() {
        return x+" "+y+" "+cluster+" "+(isNoised?1:0);
    }

}
