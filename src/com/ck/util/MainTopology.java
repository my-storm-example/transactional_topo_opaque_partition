package com.ck.util;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.transactional.TransactionalTopologyBuilder;

import com.ck.bolt.TransactionBolt;
import com.ck.bolt.TransactionCommitBolt;
import com.ck.spout.TransactionalSpout;


public class MainTopology {

  /**
   * @Title: main
   * @Description: TODO
   * @param args
   * @return: void
   */
  @SuppressWarnings("deprecation")
  public static void main(String[] args) {

    RabbitMQComponent rabbitMQComponent =
        new RabbitMQComponent("192.168.0.88", 5672, "root", "123456", "log_monitor_mq");

    TransactionalTopologyBuilder ttb =
        new TransactionalTopologyBuilder("TransactionTopo", "transactionalSpout",
            new TransactionalSpout(rabbitMQComponent), 1);

    ttb.setBolt("processBolt", new TransactionBolt(), 2).shuffleGrouping("transactionalSpout");
    ttb.setBolt("commitBolt", new TransactionCommitBolt()).globalGrouping("processBolt");

    Config conf = new Config();

    // 此处设置为debug，适合我们开发阶段，每发送一个tuple，都会在控制台打印信息。
    conf.setDebug(false);

    if (args == null || args.length <= 0) {
      LocalCluster lc = new LocalCluster();
      lc.submitTopology("mytopology", conf, ttb.buildTopology());
    } else {
      try {
        StormSubmitter.submitTopology(args[0], conf, ttb.buildTopology());
      } catch (AlreadyAliveException e) {
        e.printStackTrace();
      } catch (InvalidTopologyException e) {
        e.printStackTrace();
      }
    }

  }
}
