package distances;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

public class ClosestK {

	public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {

		int n = 500000;
		int k = 10;
		int threads = 4;

		Random r = new Random();
		Hotel[] hotels = new Hotel[n];
		IntStream.range(0, n).forEach(i -> hotels[i] = new Hotel(i, r));

		ExecutorService pool = Executors.newFixedThreadPool(threads);
		List<Future> results = new LinkedList<>();
		int segment = n/threads;
		for(int i=0; i<n; i+=segment) {
			int from = i;
			int to = i + segment - 1;
			results.add(pool.submit(() -> {
				long ini = System.currentTimeMillis();
				logBegin(from, to, hotels);
				new ClosestK(n, k, from, to, hotels);
				logEnd(n, k, ini);
			}));
		}
		for(Future f : results)
			f.get(2, TimeUnit.HOURS);
		pool.shutdown();
	}

	public ClosestK(int N, int k, int from, int to, Hotel[] hotels) {

		float[] distances = new float[N];
		int[] hotelIndexes = new int[N];
		IntStream.range(0, N).forEach(i -> hotelIndexes[i] = i);

		try (final FileWriter fw = new FileWriter(new File("result.csv"))) {
			
			IntStream.range(from, to).forEach(i -> {

				long ini = System.currentTimeMillis();
				
				Hotel h = hotels[i];
				// fill distances from this hotel
				euclideanDistances(h, hotels, distances);
				// calculate top k nearest on hotelIndexes
				topK(k, hotelIndexes, distances);
				// print result
				writeToCSV(k, fw, h.id, hotelIndexes);

				logStep(i, ini);
					
			});

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	private void euclideanDistances(Hotel a, Hotel[] hotels, float[] distances) {
		IntStream.range(0, hotels.length).forEach( j -> {
					Hotel b = hotels[j];
					distances[b.id] = (a.lat - b.lat) * (a.lat - b.lat)
							+ (a.lon - b.lon) * (a.lon - b.lon);
				});
	}

	private void writeToCSV(int k, FileWriter fw, int hotelId, int[] hotels) {
		try {
			fw.append(Integer.toString(hotelId));
			fw.append(',');
			for (int i = 0; i < k; i++) {
				fw.append(Integer.toString(hotels[i]));
				fw.append(',');
			}
			fw.append('\n');

		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	// top-k quick-select impl
	private void topK(int k, int[] hotels, float[] distances) {

		int left = 0;
		int right = hotels.length - 1;

		while (left < right) {
			int p = partition(hotels, distances, left, right);
			if (p < k)
				left = p + 1;
			else if (p > k)
				right = p - 1;
			else
				break;
		}
	}

	private int partition(int[] hotels, float[] distances, int left, int right) {

		int lo = left;
		int hi = right + 1;
		double pElem = distances[hotels[left]];
		while (lo < hi) {
			while (distances[hotels[++lo]] <= pElem && lo != right);
			while (distances[hotels[--hi]] > pElem && hi != left);
			if (lo < hi)
				swap(hotels, lo, hi);
		}
		swap(hotels, left, hi);
		return hi;
	}

	private void swap(int[] hotels, int i, int j) {
		int temp = hotels[i];
		hotels[i] = hotels[j];
		hotels[j] = temp;
	}
	
	private static void logBegin(int from, int to, Hotel[] hotels) {
		System.out.println(String.format("segment between %sand %s h of a total of %s h", from, to, hotels.length));
	}
	
	private void logStep(int i, long ini) {
		if (i % 100 == 0) System.out.println(String.format("tooked for id [%s]: %s ms", i, System.currentTimeMillis() - ini));
	}
	
	private static void logEnd(int n, int k, long ini) {
		System.out.println(String.format("for n = %s and k = %s, tooked: %s ms", n, k, System.currentTimeMillis() - ini));
	}
}

class Hotel {
	final int id;
	final float lat;
	final float lon;
	private final static int maxLat = 360;
	private final static int maxLon = 180;

	public Hotel(int id, Random r) {
		this.id = id;
		this.lat = r.nextFloat() * r.nextInt(maxLat);
		this.lon = r.nextFloat() * r.nextInt(maxLon);
	}
}
