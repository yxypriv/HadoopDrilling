package knn.model;

public class KNNNode {
	double[] features;
	String label;

	public KNNNode(String line) {
		String[] split = line.split(",");
		features = new double[split.length - 1];
		for (int i = 0; i < split.length; i++) {
			if (i < split.length - 1) {
				features[i] = Double.parseDouble(split[i]);
			} else {
				label = split[i];
			}
		}
	}
	
	public double[] getFeatures() {
		return features;
	}

	public void setFeatures(double[] features) {
		this.features = features;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

}
