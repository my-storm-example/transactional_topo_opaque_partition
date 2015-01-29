package com.ck.spout;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.ck.pojo.TransactionalMeta;
import com.ck.util.RabbitMQComponent;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class TransactionalSpout implements IOpaquePartitionedTransactionalSpout<TransactionalMeta> {

  /**
   * @fieldName: serialVersionUID
   * @fieldType: long
   * @Description: TODO
   */
  private static final long serialVersionUID = 1L;

  private final int _amount = 50;

  private static RabbitMQComponent rabbitMQComponent;

  private final Map<Integer, String> _logMap = new HashMap<Integer, String>();

  private transient static Channel channel;

  private transient static QueueingConsumer amqpConsumer;

  private static QueueingConsumer.Delivery delivery = null;

  public TransactionalSpout(RabbitMQComponent rabbitMQComponent) {
    this.rabbitMQComponent = rabbitMQComponent;
  }

  private void setupEvn() {
    try {
      channel = rabbitMQComponent.getChannel();
      channel.basicQos(50);
      channel.queueDeclare("logqueue", true, false, false, null);
      this.amqpConsumer = new QueueingConsumer(channel);
      channel.basicConsume("logqueue", false, amqpConsumer);
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("连接rabbitmq失败！");
    }
  }

  public Map<Integer, String> logSource() {
    Map<Integer, String> logMap = new HashMap<Integer, String>();
    for (int i = 0; i < 100; i++) {
      logMap.put(i, "log");
    }
    return logMap;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("txid", "log"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  @Override
  public backtype.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout.Emitter<TransactionalMeta> getEmitter(
      Map conf, TopologyContext context) {
    return new Emmit();
  }

  @Override
  public backtype.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout.Coordinator getCoordinator(
      Map conf, TopologyContext context) {
    return new Coordinator();
  }

  /**
   * @author xuer
   * @date 2014-8-27 - 上午10:42:37
   * @Description 用numPartitions方法，返回此分区事务topo有多少个分区
   */
  class Coordinator implements IOpaquePartitionedTransactionalSpout.Coordinator {

    private final List<String> logList = new ArrayList<String>();

    String log;

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void close() {}

  }

  class Emmit implements IOpaquePartitionedTransactionalSpout.Emitter<TransactionalMeta> {

    @Override
    public TransactionalMeta emitPartitionBatch(TransactionAttempt tx,
        BatchOutputCollector collector, int partition, TransactionalMeta lastPartitionMeta) {

      long waitTimeout = 1000L;
      long startTime;
      long endTime;
      byte[] logByte;
      long _index;
      int _current_amount = _amount;
      String logStr;
      int count = 0;

      setupEvn();

      // 处理分区的元数据
      if (lastPartitionMeta == null) {
        _index = 0;
      } else {
        _index = lastPartitionMeta.getIndex() + lastPartitionMeta.getAmount();
      }

      System.out.println("事务开始的partition：" + partition + ",此事务的TransactionalId:"
          + tx.getTransactionId() + ",index:" + _index + ",amount:" + _amount);

      startTime = System.currentTimeMillis();
      for (int i = 0; i < _amount; i++) {
        try {
          // 如果nextDelivery()没加参数，那么此处会一直等待，直到返回一条消息,参数是毫秒，如果写1毫秒，就可以忽略为不等待了。
          delivery = amqpConsumer.nextDelivery(waitTimeout);// 此处可以这么设计，消息等1s，如果1s没有消息了，那么就直接发送已经收到的消息
          endTime = System.currentTimeMillis();
          waitTimeout = waitTimeout - (endTime - startTime);// 计算出下一次需要等待多少毫秒
        } catch (ShutdownSignalException | ConsumerCancelledException | InterruptedException e) {
          e.printStackTrace();
        }

        if (delivery != null) {// 接收到了消息，那么就直接发送
          count++;
          logByte = delivery.getBody();
          logStr = new String(logByte, Charset.forName("utf-8"));
          collector.emit(new Values(tx, logStr));
        }

        if (waitTimeout <= 0) {
          if (count > 0)// 如果等待的时间过了1s，那么此分区此批次直接发送i条数据
          {
            _current_amount = count;
            return new TransactionalMeta(_index, _current_amount);
          } else// 如果1s中没有接受到任何数据，那么就继续循环等待
          {
            waitTimeout = 1000L;
          }
        }

      }

      return new TransactionalMeta(_index, _current_amount);
    }

    @Override
    public int numPartitions() {
      return 1;
    }

    @Override
    public void close() {}

  }

}
