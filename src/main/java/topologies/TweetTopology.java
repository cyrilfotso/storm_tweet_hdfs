package topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import bolt.CountBolt;
import bolt.ParseTweetBolt;
import bolt.ReportBolt;
import spout.TweetSpout;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

class TweetTopology {
	/**
	 * 
	 * @param args
	 *            "[Your customer key]" "[Your secret key]" 
	 *            "[Your access token]" "[Your access secret]" 
	 *            "[Your NameNode IP]" "[Your HDFS PATH]" 
	 *            "[Your Toplogy Name]" "[the choosen mode 'cluster' or 'local']"
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// create the topology
		TopologyBuilder builder = new TopologyBuilder();

		/*
		 * In order to create the spout, you need to get twitter credentials If
		 * you need to use Twitter firehose/Tweet stream for your idea, create a
		 * set of credentials by following the instructions at
		 *
		 * https://www.slickremix.com/docs/how-to-get-api-keys-and-tokens-for-twitter/
		 *
		 */

		// Initialisation of the global params to have a working topology

		String Your_customer_key  =  (args != null && args.length > 0) ? args[0] : "Your_Default_customer_key";
		String Your_secret_key    =  (args != null && args.length > 1) ? args[1] : "Your_Default_secret_key";
		String Your_access_token  =  (args != null && args.length > 2) ? args[2] : "Your_Default_access_token";
		String Your_access_secret =  (args != null && args.length > 3) ? args[3] : "Your_Default_access_secret";
		String hostname           =  (args != null && args.length > 4) ? args[4] : "127.0.0.1";
		String hdfsOutputDir      =  (args != null && args.length > 5) ? args[5] : "tmp";		
		String topologyName       =  (args != null && args.length > 6) ? args[6] : "tweet-word-count";
		String runningMode        =  (args != null && args.length > 7) ? args[7] : "local";
		String portNumber         =  (args != null && args.length > 8) ? args[8] :"8020";


		
		// now instanciate the tweet spout with the credentials

		TweetSpout tweetSpout = new TweetSpout(Your_customer_key, Your_secret_key, Your_access_token,
				Your_access_secret);

		// initialisation of the HDFS Bolt

		// Sync with FileSystem after every 100 tuples.
		SyncPolicy syncPolicy = new CountSyncPolicy(100);

		RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");

		// Rotate files after each 128MB
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(128.0f, Units.MB);

		// FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/tmp/"); // if we want to use the default extension (usualy .txt)
		FileNameFormat fileNameFormat = new DefaultFileNameFormat()
				.withExtension(".csv")
				.withPath("/" + hdfsOutputDir + "/");

		HdfsBolt hdfsbolt = new HdfsBolt().withFsUrl("hdfs://" + hostname + ":"+portNumber).withFileNameFormat(fileNameFormat)
				.withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);

		// End Initialisation of the hdfs bolt

		// attach the tweet spout to the topology - parallelism of 1
		builder.setSpout("tweet-spout", tweetSpout, 1);

		// attach the parse tweet bolt using shuffle grouping
		builder.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 10).shuffleGrouping("tweet-spout");

		// attach the count bolt using fields grouping - parallelism of 15
		builder.setBolt("count-bolt", new CountBolt(), 15).fieldsGrouping("parse-tweet-bolt", new Fields("tweet-word"));

		// attach the hdfs bolt using global grouping - parallelism of 1
		builder.setBolt("hdfs-bolt", hdfsbolt, 1).shuffleGrouping("count-bolt");

		// attach the report bolt using global grouping - parallelism of 1; this
		// bolt save data to redis server
		// builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("count-bolt");

		// create the default config object
		Config conf = new Config();

		// set the config in debugging mode
		conf.setDebug(true);

		if (args != null && args.length > 0 && runningMode.replace(" ", "").toLowerCase().equals("cluster")) {

			// run it in a live cluster

			// set the number of workers for running all spout and bolt tasks
			conf.setNumWorkers(3);

			// create the topology and submit with config
			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());

		} else {

			// run it in a simulated local cluster

			// set the number of threads to run - similar to setting number of
			// workers in live cluster
			conf.setMaxTaskParallelism(3);

			// create the local cluster instance
			LocalCluster cluster = new LocalCluster();

			// submit the topology to the local cluster
			cluster.submitTopology("tweet-word-count", conf, builder.createTopology());

			// let the topology run for 300 seconds. note topologies never
			// terminate!
			Utils.sleep(300000);

			// now kill the topology
			cluster.killTopology(topologyName);

			// we are done, so shutdown the local cluster
			cluster.shutdown();
		}
	}
}
