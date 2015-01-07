package distances;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.stream.IntStream;

public class Distances {
	
	private final QuickSelectIndexedTopK topK = new QuickSelectIndexedTopK();

	public Distances(int N, int k, Hotel[] hotelsFrom, Hotel[] hotelsTo) {

		float[] distances = new float[N];
		int[] hotelIndexes = new int[N];
		IntStream.range(0, N).forEach(i -> hotelIndexes[i] = i);

		try (final FileWriter fw = new FileWriter(new File("result.csv"))) {
			IntStream.range(0, hotelsFrom.length).forEach(
					hotelId -> {
						long ini = System.currentTimeMillis();
						int[] result = this.topK(k, hotelsFrom[hotelId],
								hotelsTo, hotelIndexes, distances);
						writeToCSV(k, fw, hotelId, result);
						if(hotelId % 100 == 0)
							System.out.println(String.format("tooked for id [%s]: %s ms", hotelId, System.currentTimeMillis() - ini));
					});

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	private void writeToCSV(int k, FileWriter fw, int hotelId,
			int[] hotelIndexes) {
		try {
			fw.append(Integer.toString(hotelId));
			fw.append(',');
			for (int i = 0; i < k; i++) {
				fw.append(Integer.toString(hotelIndexes[i]));
				fw.append(',');
			}
			fw.append('\n');

		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	public int[] topK(int k, Hotel h, Hotel[] all, int[] hotelIndexes, float[] distances) {
		for (int i = 0; i < all.length; i++) {
			Hotel other = all[i];
			distances[other.id] = euclideanDistance(h.lat, h.lon, other.lat, other.lon);
		}
		return this.topK.topK(k, hotelIndexes, distances);
	}

	private float euclideanDistance(float f, float g, float h, float i) {
		return (f - h) * (f - h) + (g - i) * (g - i);
	}

}

class Hotel {
	final int id;
	final float lat;
	final float lon;

	public Hotel(int id, float lat, float lon) {
		this.id = id;
		this.lat = lat;
		this.lon = lon;
	}
}
