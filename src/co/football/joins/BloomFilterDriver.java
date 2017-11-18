package co.football.joins;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.apache.log4j.BasicConfigurator;

public class BloomFilterDriver {
	public static void main(String[] args) throws Exception {
		 BasicConfigurator.configure();
		if(args.length!=4) {
			System.err.println("Usage BloomFilterDriver: <inputfile> <numeber of members> <false positive rate> <bloom filter destination>");
			System.exit(2);
		}
		// Parse command line arguments
		Path inputFile = new Path(args[0]);
		int numMembers = Integer.parseInt(args[1]);
		float falsePosRate = Float.parseFloat(args[2]);
		Path bfFile = new Path(args[3]);
		// Calculate our vector size and optimal K value based on approximations
		int vectorSize = getOptimalBloomFilterSize(numMembers, falsePosRate);
		int nbHash = getOptimalK(numMembers, vectorSize);
		// Create new Bloom filter
		int i=0;
		System.out.println(i);
		BloomFilter filter = new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);
		System.out.println("Training Bloom filter of size " + vectorSize + " with " + nbHash + " hash functions, "
				+ numMembers + " approximate number of records, and " + falsePosRate + " false positive rate");
		int numElements = 0;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.newInstance(conf);
		
		FSDataInputStream inputStream = fs.open(inputFile);
		BufferedReader b = new BufferedReader(new InputStreamReader(inputStream));
		String s = null;
		while ((s = b.readLine()) != null) {
			filter.add(new Key(s.split(",")[0].getBytes()));
			++numElements;
		}
		b.close();
		System.out.println("Trained Bloom filter with " + numElements + " entries.");

		System.out.println("Serializing Bloom filter to HDFS at " + bfFile);
		FSDataOutputStream strm = fs.create(bfFile);
		filter.write(strm);
		strm.flush();
		strm.close();

		System.exit(0);
	}

	/**
	 * Gets the optimal Bloom filter sized based on the input parameters and the
	 * optimal number of hash functions.
	 *
	 * @param numElements
	 *            The number of elements used to train the set.
	 * @param falsePosRate
	 *            The desired false positive rate.
	 * @return The optimal Bloom filter size.
	 */
	public static int getOptimalBloomFilterSize(int numElements, float falsePosRate) {
		return (int) (-numElements * (float) Math.log(falsePosRate) / Math.pow(Math.log(2), 2));
	}

	/**
	 * Gets the optimal-k value based on the input parameters.
	 *
	 * @param numElements
	 *            The number of elements used to train the set.
	 * @param vectorSize
	 *            The size of the Bloom filter.
	 * @return The optimal-k value, rounded to the closest integer.
	 */
	public static int getOptimalK(float numElements, float vectorSize) {
		return (int) Math.round(vectorSize * Math.log(2) / numElements);
	}
}
