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

class TweetTopology
{
  public static void main(String[] args) throws Exception
  {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    /*
     * In order to create the spout, you need to get twitter credentials
     * If you need to use Twitter firehose/Tweet stream for your idea,
     * create a set of credentials by following the instructions at
     *
     * https://dev.twitter.com/discussions/631
     *
     */

    // now create the tweet spout with the credentials
    TweetSpout tweetSpout = new TweetSpout(
//        "[Your customer key]",
//        "[Your secret key]",
//        "[Your access token]",
//        "[Your access secret]"
    		"kagDL5PQeW9M4n4L3PUnxvSqn",
            "ymRNbZdIxgsOgcIcI9vfyhkyhAxmicm4zPx8ZJdhbMIYaYW8hE",
            "991625774-8hI0qC9qbj1Opqe9hI78vjo94SGsm8n8kRCq8Kd3",
            "fcrXZ4qFO7Za5aYg8jilYilVrKihrSytPakftFPs9XiXD"
    );
    
    // initialisation of the HDFS Bolt 
    
    String hdfsOutputDir="tmp";
    String hostname="54.193.49.124";

 // Sync with FileSystem after every 100 tuples.
    SyncPolicy syncPolicy = new CountSyncPolicy(100);
    
    RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");
    
    // Rotate files after each 127MB
    FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);
    
    // Use default, Storm-generated file names
//    FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/tmp/");
    FileNameFormat fileNameFormat = new DefaultFileNameFormat().withExtension(".csv").withPath("/tmp/");
//    
//    HdfsBolt hdfsbolt = new HdfsBolt()
//            .withFsUrl("hdfs://" + hostname + ":8020") //54.193.49.124:8020/tmp/
//            .withFileNameFormat(fileNameFormat)
//            .withRecordFormat(format)
//            .withRotationPolicy(rotationPolicy)
//            .withSyncPolicy(syncPolicy);
    
    HdfsBolt hdfsbolt = new HdfsBolt()
            .withFsUrl("hdfs://54.193.49.124:8020")
            .withFileNameFormat(fileNameFormat)
            .withRecordFormat(format)
            .withRotationPolicy(rotationPolicy)
            .withSyncPolicy(syncPolicy);
    
    // End Initialisation of the hdfs bolt

    // attach the tweet spout to the topology - parallelism of 1
    builder.setSpout("tweet-spout", tweetSpout, 1);
    
    builder.setBolt("hdfs-bolt", hdfsbolt, 1).shuffleGrouping("tweet-spout");

    // attach the parse tweet bolt using shuffle grouping
    //builder.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 10).shuffleGrouping("tweet-spout");

    // attach the count bolt using fields grouping - parallelism of 15
    //builder.setBolt("count-bolt", new CountBolt(), 15).fieldsGrouping("parse-tweet-bolt", new Fields("tweet-word"));

    // attach the report bolt using global grouping - parallelism of 1
    //builder.setBolt("hdfs-bolt", hdfsbolt, 1).shuffleGrouping("count-bolt");
    
    // attach the report bolt using global grouping - parallelism of 1
    //builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("count-bolt");
    
    // create the default config object
    Config conf = new Config();

    // set the config in debugging mode
    conf.setDebug(true);

    if (args != null && args.length > 0) {

      // run it in a live cluster

      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers(3);

      // create the topology and submit with config
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

      } else {

      // run it in a simulated local cluster

      // set the number of threads to run - similar to setting number of workers in live cluster
      conf.setMaxTaskParallelism(3);

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      cluster.submitTopology("tweet-word-count", conf, builder.createTopology());

      // let the topology run for 300 seconds. note topologies never terminate!
      Utils.sleep(300000);

      // now kill the topology
      cluster.killTopology("tweet-word-count");

      // we are done, so shutdown the local cluster
      cluster.shutdown();
    }
  }
}
