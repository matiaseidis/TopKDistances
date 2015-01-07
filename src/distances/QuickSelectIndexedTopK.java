package distances;

import java.util.Random;

public class QuickSelectIndexedTopK {
	
	private static final Random r = new Random();

	public int[] topK(int k, int[] hotels, float[] distances) {
		
//		shuffle(hotels);
		
		int left = 0;
		int right = hotels.length - 1;
		
		while(left < right) {
			int p = partition(hotels, distances, left, right);
			if(p < k) left = p + 1;
			else if(p > k) right = p - 1;
			else break;
		}
		
		return hotels;
	}

	private int partition(int[] hotels, float[] distances, int left, int right) {
		
		int lo = left;
		int hi = right + 1;
		double pElem = distances[hotels[left]];
		while(lo < hi) {
			while(distances[hotels[++lo]] <= pElem && lo != right);
			while(distances[hotels[--hi]] > pElem && hi != left);
			if(lo < hi)
				swap(hotels, lo, hi);
		}
		swap(hotels, left, hi);
		return hi;
	}

	private void shuffle(int[] hotels) {
		for(int i = 0; i< hotels.length; i++)
			swap(hotels, r.nextInt(hotels.length), i);
	}

	private void swap(int[] hotels, int i, int j) {
		int temp = hotels[i];
		hotels[i] = hotels[j];
		hotels[j] = temp;
	}

}
