package distances;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
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
		Hotel[] hotels = IntStream.range(0, n).mapToObj(i-> new Hotel(i, r)).toArray(Hotel[]::new);

		ExecutorService pool = Executors.newFixedThreadPool(threads);
		List<Future> results = new LinkedList<>();
		int segment = n/threads;
		long ini = System.currentTimeMillis();
		logBegin(hotels, k, threads);
		for(int i=0; i<n; i+=segment) {
			int from = i;
			int to = i + segment;
			results.add(pool.submit(() -> {
				new ClosestK(n, k, from, to, hotels);
				logSegment(from, to);
			}));
		}
		for(Future f : results)
			f.get(2, TimeUnit.HOURS);
		logEnd(n, k, ini);
		pool.shutdown();
	}

	public ClosestK(int n, int k, int from, int to, Hotel[] hotels) {

		final float[] distances = new float[n];
		final int[] hotelIndexes = IntStream.range(0, n).toArray();
		final Integer[] sortedResult = new Integer[k];

		try (final FileWriter fw = new FileWriter(new File("result.csv"))) {
			
			IntStream.range(from, to).forEach(i -> {

				long ini = System.currentTimeMillis();
				
				Hotel h = hotels[i];
				// fill distances from this hotel
				euclideanDistances(h, hotels, distances);
				// calculate top k nearest on hotelIndexes
				topK(k, hotelIndexes, distances);
				// sort the top k nearest
				IntStream.range(0,  k).forEach(e -> sortedResult[e] = hotelIndexes[e]);
				Arrays.sort(sortedResult, 0, k, (a,b) -> Float.compare(distances[a],distances[b]));
				// print result
				writeToCSV(k, fw, h.id, sortedResult);

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

	private void writeToCSV(int k, FileWriter fw, int hotelId, Integer[] hotels) {
		try {
			fw.append(Integer.toString(hotelId));
			fw.append(',');
			for (int i = 0; i < k; i++) {
				fw.append(hotels[i].toString());
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
	
	private static void logBegin(Hotel[] hotels, int k, int threads) {
		System.out.println(String.format("START | n=%s  and k=%s. threads: %s", hotels.length, k, threads));
	}
	
	private static void logSegment(int from, int to) {
		System.out.println(String.format("done segment[%s...%s]", from, to));
	}
	
	private void logStep(int i, long ini) {
		if (i % 500 == 0) System.out.println(String.format("for id [%s]: %s ms", i, System.currentTimeMillis() - ini));
	}
	
	private static void logEnd(int n, int k, long ini) {
		long elapsed = System.currentTimeMillis() - ini;
		System.out.println(String.format("END   | n=%s and k=%s, tooked: %s ms (%s min(s))", n, k, elapsed, elapsed/60000));
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
