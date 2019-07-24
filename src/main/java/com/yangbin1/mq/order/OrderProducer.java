package com.yangbin1.mq.order;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @ClassName: OrderProducer
 * @Auther: yangbin1
 * @Date: 2019/7/22 15:53
 * @Description:
 */
public class OrderProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {

        //1.创建DefaultMQProducer
        DefaultMQProducer producer = new DefaultMQProducer("demo_producer_group");

        //2.nameSrvAddress
        producer.setNamesrvAddr("47.101.10.100:9876");

        //3.开启producer
        producer.start();

        //4.创建消息对象
        Message message = new Message("Topic_Order_Demo","Tags","Keys_1","hello".getBytes(RemotingHelper.DEFAULT_CHARSET));

        //5.发送消息
        // 第一个参数发送的消息，
        // 第二个参数指定队列
        // 指定队列下标
        for (int i = 0; i < 5; i++) {
            SendResult result = producer.send(message, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            // 获取队列的下标
                            Integer index = (Integer) arg;
                            //获取对应下标的队列
                            return mqs.get(index);
                        }
                    },
                    0);
            System.out.println(result);
        }
        //关闭
        producer.shutdown();

    }
}
