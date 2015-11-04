package org.petar.spark.exampleswTwitterStreaming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import com.google.common.base.Optional;

import scala.Tuple2;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

public class TwitterStreamingExamples {
	public static void main(String[] args) {
		twitterStreaming();	
	}

	private static void twitterStreaming() {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		Configuration twitterConf = ConfigurationContext.getInstance();
		Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);
		SparkConf sparkConf = new SparkConf().setAppName("Tweets Spark").setMaster("local[2]");
		JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(30000));
		sc.checkpoint("/tmp/check");

		JavaReceiverInputDStream<Status> twitts = TwitterUtils.createStream(sc, twitterAuth);

		JavaDStream<HashtagEntity[]> map = twitts.map(new Function<Status, HashtagEntity[]>() {
			public HashtagEntity[] call(Status arg0) throws Exception {
				return arg0.getHashtagEntities();
			}
		});

		JavaPairDStream<String, Integer> setOfHashtags = map
				.flatMapToPair(new PairFlatMapFunction<HashtagEntity[], String, Integer>() {
					public Iterable<Tuple2<String, Integer>> call(HashtagEntity[] arg0) throws Exception {
						ArrayList<Tuple2<String, Integer>> tuples = new ArrayList<Tuple2<String, Integer>>();
						for (HashtagEntity hashtag : arg0) {
							tuples.add(new Tuple2<String, Integer>(hashtag.getText(), 1));
						}
						return tuples;
					}
				});
		

		JavaPairDStream<String, Integer> hashtagCount = setOfHashtags
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer arg0, Integer arg1) throws Exception {
						// TODO Auto-generated method stub
						return arg0 + arg1;
					}
				}).cache();

		hashtagCount.mapToPair(SWAP_TUPLE).foreachRDD(PRINT_FOREACH_RDD);

		hashtagCount.updateStateByKey(UPDATE_HASHTAGS_COUNT).mapToPair(SWAP_TUPLE).foreachRDD(PRINT_FOREACH_RDD);

		sc.start();
		sc.awaitTermination();
	}

	private static Function2<List<Integer>, Optional<Integer>, Optional<Integer>> UPDATE_HASHTAGS_COUNT = new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
		public Optional<Integer> call(List<Integer> arg0, Optional<Integer> arg1) throws Exception {
			int sum = arg1.or(0);
			for (int i : arg0) {
				sum += i;
			}
			return Optional.of(sum);
		}
	};

	private static PairFunction<Tuple2<String, Integer>, Integer, String> SWAP_TUPLE = new PairFunction<Tuple2<String, Integer>, Integer, String>() {
		public Tuple2<Integer, String> call(Tuple2<String, Integer> arg0) throws Exception {
			return arg0.swap();
		}
	};

	private static Function<JavaPairRDD<Integer, String>, Void> PRINT_FOREACH_RDD = new Function<JavaPairRDD<Integer, String>, Void>() {
		public Void call(JavaPairRDD<Integer, String> arg0) throws Exception {
			JavaPairRDD<Integer, String> sortByKey = arg0.sortByKey(false);
			List<Tuple2<Integer, String>> take = sortByKey.take(5);
			System.out.println("");
			for (Tuple2<Integer, String> top : take) {
				System.out.println(top._2() + "\t: " + top._1());
			}
			return null;
		}
	};
}
