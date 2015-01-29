package com.ck.util;

import java.io.IOException;
import java.io.Serializable;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQComponent implements Serializable {

  private static final long serialVersionUID = -154748059618033665L;
  private String host;
  private int port;
  private String username;
  private String password;
  private String vhost;

  public RabbitMQComponent(String host, int port, String username, String password, String vhost) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.vhost = vhost;
  }

  public Channel getChannel() throws IOException {
    final ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost(host);
    connectionFactory.setPort(port);
    connectionFactory.setUsername(username);
    connectionFactory.setPassword(password);
    connectionFactory.setVirtualHost(vhost);
    Connection connection = connectionFactory.newConnection();
    return connection.createChannel();
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getVhost() {
    return vhost;
  }

  public void setVhost(String vhost) {
    this.vhost = vhost;
  }

}
