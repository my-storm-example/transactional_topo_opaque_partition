package com.ck.bolt;

import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class TransactionBolt extends BaseTransactionalBolt {

  /**
   * @fieldName: serialVersionUID
   * @fieldType: long
   * @Description: TODO
   */
  private static final long serialVersionUID = 1L;

  BatchOutputCollector _collector;

  TransactionAttempt _id;

  int count = 0;

  @Override
  public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector,
      TransactionAttempt id) {
    _collector = collector;
    _id = id;
  }

  /**
   * @Title: execute
   * @Description: 每个tuple都会执行此方法,我比较有疑问的是，count会不会每次都清零，count什么时候会清零？
   * @param tuple
   */
  @Override
  public void execute(Tuple tuple) {
    count++;
    // 此处可以用来处理日记信息，因为tuple从那里发送过来的是new Values("txid", "log")
  }

  /**
   * @Title: finishBatch
   * @Description: 当次批次执行完了之后，就会执行这个方法，把计算到的此批次的tuple总数发送给下一个bolt
   */
  @Override
  public void finishBatch() {
    System.err.println("txid:" + _id.getTransactionId() + ",count:" + count);
    _collector.emit(new Values(_id, count));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("tx", "count"));
  }

}
