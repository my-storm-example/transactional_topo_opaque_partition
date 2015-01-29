package Example;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.coordination.IBatchBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MyDailyBatchBolt implements IBatchBolt<TransactionAttempt> {

  /**
	 * 
	 */
  private static final long serialVersionUID = 1L;
  Map<String, Integer> countMap = new HashMap<String, Integer>();
  BatchOutputCollector collector;
  Integer count = null;
  String today = null;
  TransactionAttempt tx = null;

  @Override
  public void execute(Tuple tuple) {
    // TODO Auto-generated method stub
    String log = tuple.getString(1);
    tx = (TransactionAttempt) tuple.getValue(0);
    if (log != null && log.split("\\t").length >= 3) {
      today = DateFmt.getCountDate(log.split("\\t")[2], DateFmt.date_short);
      count = countMap.get(today);
      if (count == null) {
        count = 0;
      }
      count++;

      countMap.put(today, count);
    }
  }

  @Override
  public void finishBatch() {
    System.err.println("111111111111111111111111111111" + tx + "--" + today + "--" + count);
    collector.emit(new Values(tx, today, count));
  }

  @Override
  public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector,
      TransactionAttempt id) {
    // TODO Auto-generated method stub
    this.collector = collector;

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // TODO Auto-generated method stub
    declarer.declare(new Fields("tx", "date", "count"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    // TODO Auto-generated method stub
    return null;
  }

}
