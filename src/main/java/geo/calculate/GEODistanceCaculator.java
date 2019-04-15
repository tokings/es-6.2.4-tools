package geo.calculate;

import java.io.Serializable;

public class GEODistanceCaculator {

	private static final double DEFAULT_EARTH_RADIUS = Ellipsoid.Sphere.getSemiMajorAxis();
	private static final int DEFAULT_ITERATE_CNT = 20;
	private static final double PI_OVER_180 = Math.PI / 180.0;

	public static void main(String[] args) {

//		double lat1 = 29.490295, lng1 = 106.486654, lat2 = 29.615467, lng2 = 106.581515;
		double lat1 = 0.370860921, lng1 = 0.990725128, lat2 = 1.318451439, lng2 = 1.694006323;

		// get constants
		double a = Ellipsoid.Sphere.getSemiMajorAxis();
		double b = Ellipsoid.Sphere.getSemiMinorAxis();
		double f = Ellipsoid.Sphere.getFlattening();

		long caculateCnt = 1;
		long start = System.currentTimeMillis();
		double meter2;
		for (int i = 0; i < caculateCnt; i++) {
			meter2 = calculateGeodeticCurve(DEFAULT_ITERATE_CNT, a, b, f, lat1, lng1, lat2, lng2);
			 System.out.println(meter2);
		}
		long end = System.currentTimeMillis();
		System.out.println("total used " + (end - start) + " ms.");
		System.out.println("generated " + caculateCnt / (end - start) * 1000 + " distances per second.");

		start = System.currentTimeMillis();
		for (int i = 0; i < caculateCnt; i++) {
			meter2 = distanceOfTwoGEOPoints(Ellipsoid.Sphere.getSemiMajorAxis(), lat1, lng1, lat2, lng2);
			 System.out.println(meter2);
		}
		end = System.currentTimeMillis();
		System.out.println("total used " + (end - start) + " ms.");
		System.out.println("generated " + caculateCnt / (end - start) * 1000 + " distances per second.");
	}

	private static double rad(double d) {
		return d * Math.PI / 180.0;
	}

	/**
	 * 计算精度一般、效率高(24390000/s)，采用圆计算
	 * 
	 * @param earthRadius
	 * @param lat1
	 * @param lng1
	 * @param lat2
	 * @param lng2
	 * @return
	 */
	public static double distanceOfTwoGEOPoints(double earthRadius, double lat1, double lng1, double lat2,
			double lng2) {
		double radLat1 = rad(lat1);
		double radLat2 = rad(lat2);
		double a = radLat1 - radLat2;
		double b = rad(lng1) - rad(lng2);
		double s = 2 * Math.asin(Math.sqrt(
				Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
		s = s * earthRadius;
		s = Math.round(s * 10000) / 10000;
		return s;
	}

	static public double toRadians(double degrees) {
		return degrees * PI_OVER_180;
	}

	/**
	 * 计算精度高、效率一般(1184000/s)，采用椭圆计算
	 * 
	 * @param iterateCnt
	 * @param a
	 * @param b
	 * @param f
	 * @param lat1
	 * @param lng1
	 * @param lat2
	 * @param lng2
	 * @return
	 */
	public static double calculateGeodeticCurve(int iterateCnt, double a, double b, double f, double lat1, double lng1,
			double lat2, double lng2) {

		// get parameters as radians
		double phi1 = toRadians(lat1);
		double lambda1 = toRadians(lng1);
		double phi2 = toRadians(lat2);
		double lambda2 = toRadians(lng2);

		// calculations
		double a2 = a * a;
		double b2 = b * b;
		double a2b2b2 = (a2 - b2) / b2;

		double omega = lambda2 - lambda1;

		double tanphi1 = Math.tan(phi1);
		double tanU1 = (1.0 - f) * tanphi1;
		double U1 = Math.atan(tanU1);
		double sinU1 = Math.sin(U1);
		double cosU1 = Math.cos(U1);

		double tanphi2 = Math.tan(phi2);
		double tanU2 = (1.0 - f) * tanphi2;
		double U2 = Math.atan(tanU2);
		double sinU2 = Math.sin(U2);
		double cosU2 = Math.cos(U2);

		double sinU1sinU2 = sinU1 * sinU2;
		double cosU1sinU2 = cosU1 * sinU2;
		double sinU1cosU2 = sinU1 * cosU2;
		double cosU1cosU2 = cosU1 * cosU2;

		// eq. 13
		double lambda = omega;

		// intermediates we'll need to compute 's'
		double A = 0.0;
		double B = 0.0;
		double sigma = 0.0;
		double deltasigma = 0.0;
		double lambda0;

		for (int i = 0; i < iterateCnt; i++) {
			lambda0 = lambda;

			double sinlambda = Math.sin(lambda);
			double coslambda = Math.cos(lambda);

			// eq. 14
			double sin2sigma = (cosU2 * sinlambda * cosU2 * sinlambda)
					+ (cosU1sinU2 - sinU1cosU2 * coslambda) * (cosU1sinU2 - sinU1cosU2 * coslambda);
			double sinsigma = Math.sqrt(sin2sigma);

			// eq. 15
			double cossigma = sinU1sinU2 + (cosU1cosU2 * coslambda);

			// eq. 16
			sigma = Math.atan2(sinsigma, cossigma);

			// eq. 17 Careful! sin2sigma might be almost 0!
			double sinalpha = (sin2sigma == 0) ? 0.0 : cosU1cosU2 * sinlambda / sinsigma;
			double alpha = Math.asin(sinalpha);
			double cosalpha = Math.cos(alpha);
			double cos2alpha = cosalpha * cosalpha;

			// eq. 18 Careful! cos2alpha might be almost 0!
			double cos2sigmam = cos2alpha == 0.0 ? 0.0 : cossigma - 2 * sinU1sinU2 / cos2alpha;
			double u2 = cos2alpha * a2b2b2;

			double cos2sigmam2 = cos2sigmam * cos2sigmam;

			// eq. 3
			A = 1.0 + u2 / 16384 * (4096 + u2 * (-768 + u2 * (320 - 175 * u2)));

			// eq. 4
			B = u2 / 1024 * (256 + u2 * (-128 + u2 * (74 - 47 * u2)));

			// eq. 6
			deltasigma = B * sinsigma * (cos2sigmam + B / 4 * (cossigma * (-1 + 2 * cos2sigmam2)
					- B / 6 * cos2sigmam * (-3 + 4 * sin2sigma) * (-3 + 4 * cos2sigmam2)));

			// eq. 10
			double C = f / 16 * cos2alpha * (4 + f * (4 - 3 * cos2alpha));

			// eq. 11 (modified)
			lambda = omega + (1 - C) * f * sinalpha
					* (sigma + C * sinsigma * (cos2sigmam + C * cossigma * (-1 + 2 * cos2sigmam2)));

			// see how much improvement we got
			double change = Math.abs((lambda - lambda0) / lambda);

			if ((i > 1) && (change < 0.0000000000001)) {
				break;
			}
		}

		return b * A * (sigma - deltasigma);
	}
}