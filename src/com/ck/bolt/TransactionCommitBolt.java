package com.ck.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Tuple;


public class TransactionCommitBolt extends BaseTransactionalBolt implements ICommitter {

  /**
   * @fieldName: serialVersionUID
   * @fieldType: long
   * @Description: TODO
   */
  private static final long serialVersionUID = 1L;

  BatchOutputCollector _collector;

  TransactionAttempt _id;

  Long sum = 0l;

  private static List<Long> dbList = new ArrayList<Long>();
  // 这里如果没有加static，那么就会每个事务进来时都会清空此处

  private final static String GLOBAL_COUNT = "global_count";

  @Override
  public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector,
      TransactionAttempt id) {
    _collector = collector;
    _id = id;
  }

  @Override
  public void execute(Tuple tuple) {
    sum += tuple.getInteger(1);
    System.err.println("txid:" + _id.getTransactionId() + ",sum:" + sum);
  }

  @Override
  public void finishBatch() {
    Long txid = _id.getTransactionId().longValue();
    if (dbList.size() <= 0) {
      dbList.add(txid);
      dbList.add(sum);
      dbList.add(0L);
    } else {
      if (dbList.get(0) != txid) {
        dbList.add(dbList.get(1));
        dbList.set(0, txid);
        dbList.set(1, dbList.get(1) + sum);
      }
    }

    System.err.println("-------------------------------sum:" + dbList.get(1));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {}

}
