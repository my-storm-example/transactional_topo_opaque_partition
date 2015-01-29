package com.ck.pojo;

import java.io.Serializable;


public class TransactionalMeta implements Serializable {

  /**
   * @fieldName: serialVersionUID
   * @fieldType: long
   * @Description: TODO
   */
  private static final long serialVersionUID = 1L;

  private long index;

  private int amount;

  public TransactionalMeta() {}

  public TransactionalMeta(long index, Integer amount) {
    this.index = index;
    this.amount = amount;
  }

  @Override
  public String toString() {
    return "index: " + index + "; amt: " + amount;
  }

  public long getIndex() {
    return index;
  }

  public void setIndex(long index) {
    this.index = index;
  }

  public int getAmount() {
    return amount;
  }

  public void setAmount(int amount) {
    this.amount = amount;
  }

}
