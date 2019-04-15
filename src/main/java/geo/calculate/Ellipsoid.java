package geo.calculate;

import java.io.Serializable;

public class Ellipsoid implements Serializable {
	private static final long serialVersionUID = 1L;

	/** Semi major axis (meters). */
	private final double mSemiMajorAxis;

	/** Semi minor axis (meters). */
	private final double mSemiMinorAxis;

	/** Flattening. */
	private final double mFlattening;

	/** Inverse flattening. */
	private final double mInverseFlattening;

	private Ellipsoid(double semiMajor, double semiMinor, double flattening, double inverseFlattening) {
		mSemiMajorAxis = semiMajor;
		mSemiMinorAxis = semiMinor;
		mFlattening = flattening;
		mInverseFlattening = inverseFlattening;
	}

	/** The WGS84 ellipsoid. */
	static public final Ellipsoid WGS84 = fromAAndInverseF(6378137.0, 298.257223563);

	/** The GRS80 ellipsoid. */
	static public final Ellipsoid GRS80 = fromAAndInverseF(6378137.0, 298.257222101);

	/** The GRS67 ellipsoid. */
	static public final Ellipsoid GRS67 = fromAAndInverseF(6378160.0, 298.25);

	/** The ANS ellipsoid. */
	static public final Ellipsoid ANS = fromAAndInverseF(6378160.0, 298.25);

	/** The WGS72 ellipsoid. */
	static public final Ellipsoid WGS72 = fromAAndInverseF(6378135.0, 298.26);

	/** The Clarke1858 ellipsoid. */
	static public final Ellipsoid Clarke1858 = fromAAndInverseF(6378293.645, 294.26);

	/** The Clarke1880 ellipsoid. */
	static public final Ellipsoid Clarke1880 = fromAAndInverseF(6378249.145, 293.465);

	/** A spherical "ellipsoid". */
	static public final Ellipsoid Sphere = fromAAndF(6371000, 0.0);

	static public Ellipsoid fromAAndInverseF(double semiMajor, double inverseFlattening) {
		double f = 1.0 / inverseFlattening;
		double b = (1.0 - f) * semiMajor;

		return new Ellipsoid(semiMajor, b, f, inverseFlattening);
	}

	static public Ellipsoid fromAAndF(double semiMajor, double flattening) {
		double inverseF = 1.0 / flattening;
		double b = (1.0 - flattening) * semiMajor;

		return new Ellipsoid(semiMajor, b, flattening, inverseF);
	}

	public double getSemiMajorAxis() {
		return mSemiMajorAxis;
	}

	public double getSemiMinorAxis() {
		return mSemiMinorAxis;
	}

	public double getFlattening() {
		return mFlattening;
	}

	public double getInverseFlattening() {
		return mInverseFlattening;
	}
}
