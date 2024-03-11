package com.upic.servlet;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class RabbitMQPool {
  private GenericObjectPool<Channel> pool;

  public RabbitMQPool() throws Exception {
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost("44.228.45.79");
    connectionFactory.setUsername("yuhan");
    connectionFactory.setPassword("yuhan");

    this.pool = new GenericObjectPool<>(new ChannelFactory(connectionFactory));

    GenericObjectPoolConfig<Channel> config = new GenericObjectPoolConfig<>();
    config.setMaxTotal(10);
    config.setMaxIdle(8);
    config.setMinIdle(2);
    this.pool.setConfig(config);
  }

  public Channel borrowChannel() throws Exception {
    return pool.borrowObject();
  }

  public void returnChannel(Channel channel) {
    try {
      pool.returnObject(channel);
    } catch (Exception e) {
    }
  }

  public void close() {
    if (pool != null && !pool.isClosed()) {
      pool.close();
    }
  }
}

