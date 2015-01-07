package distances;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class Test {

	public static void main(String[] args) throws InterruptedException {

		int n = 500000;
		int k = 10;

		int maxLat = 360;
		int maxLon = 180;
		Random r = new Random();

		Hotel[] hotelsTo = new Hotel[n];
		for (Integer i = 0; i < n; i++)
			hotelsTo[i] = new Hotel(i, r.nextFloat() * r.nextInt(maxLat),
					r.nextFloat() * r.nextInt(maxLon));

		ExecutorService pool = Executors.newFixedThreadPool(5);
		int segment = 100000;
		IntStream.range(0, n/segment).forEach(j -> {
			Hotel[] hotelsFrom = new Hotel[segment];
			IntStream.range(0, segment).forEach(i -> hotelsFrom[i] = hotelsTo[i + (j * segment)]);
			pool.submit(test(n, k, hotelsFrom, hotelsTo));
		});

		pool.awaitTermination(1, TimeUnit.HOURS);
	}

	private static Runnable test(int n, int k, Hotel[] from, Hotel[] to) {
		return new Runnable() {

			@Override
			public void run() {
				long ini = System.currentTimeMillis();
				System.out.println(String.format(
						"start at %s for %s h from and %s h to", ini,
						from.length, to.length));
				
				new Distances(n, k, from, to);
				
				System.out.println(String.format(
						"for n = %s and k = %s, tooked: %s ms", n, k,
						System.currentTimeMillis() - ini));
			}
		};

	}

}
