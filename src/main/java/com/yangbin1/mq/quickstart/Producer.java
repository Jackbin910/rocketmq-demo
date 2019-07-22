package com.yangbin1.mq.quickstart;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * @ClassName: Producer
 * @Auther: yangbin1
 * @Date: 2019/7/22 15:53
 * @Description:
 */
public class Producer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {

        //1.创建DefaultMQProducer
        DefaultMQProducer producer = new DefaultMQProducer("demo_producer_group");

        //2.nameSrvAddress
        producer.setNamesrvAddr("47.101.10.100:9876");

        //3.开启producer
        producer.start();

        //4.创建消息对象
        Message message = new Message("Topic_Demo","Tags","Keys_1","hello".getBytes(RemotingHelper.DEFAULT_CHARSET));

        //5.发送消息
        SendResult result = producer.send(message);
        System.out.println(result);

        //关闭
        producer.shutdown();

    }
}
