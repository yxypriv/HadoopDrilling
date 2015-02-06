package knn.utils;

public class DistanceUtil {
	public static final int InnerProduct = 0;
	public static final int CosSimilarity = 1;
	public static final int EuclideanDistance = 2;
	public static final int ThreeNorm = 3;
	public static final int FourNorm = 4;
	

	public static double getDistance(double[] n1, double[] n2) {
		return getDistance(n1, n2, EuclideanDistance);
	}

	public static double getDistance(double[] n1, double[] n2, int config) {
		if (config == InnerProduct)
			return getInnerProduct(n1, n2);
		else if (config == CosSimilarity)
			return getCosSimilarity(n1, n2);
		else if (config == EuclideanDistance)
			return getEuclideanDistance(n1, n2);
		else if (config == ThreeNorm)
			return getNormDistance(n1, n2, 3);
		else if (config == FourNorm)
			return getNormDistance(n1, n2, 3);
		return -1.0;
	}

	public static double getNormDistance(double[] n1, double[] n2, int n) {
		double sum = 0.0;
		for (int i = 0; i < n1.length; i++) {
			sum += Math.pow(n1[i] - n2[i], n);
		}
		return Math.pow(sum, 1.0 / n);
	}

	public static double getEuclideanDistance(double[] n1, double[] n2) {
		// double sum = 0.0;
		// for (int i = 0; i < n1.length; i++) {
		// sum += (n1[i] - n2[i]) * (n1[i] - n2[i]);
		// }
		// return Math.sqrt(sum);
		return getNormDistance(n1, n2, 2);
	}

	public static double getInnerProduct(double[] n1, double[] n2) {
		double sum = 0.0;
		for (int i = 0; i < n1.length; i++) {
			sum += n1[i] * n2[i];
		}
		return sum;
	}

	public static double getCosSimilarity(double[] n1, double[] n2) {
		double l1 = 0.0, l2 = 0.0;
		for (int i = 0; i < n1.length; i++) {
			l1 += n1[i] * n1[i];
			l2 += n2[i] * n2[i];
		}
		l1 = Math.sqrt(l1);
		l2 = Math.sqrt(l2);
		return getInnerProduct(n1, n2) / (l1 * l2);
	}
}
