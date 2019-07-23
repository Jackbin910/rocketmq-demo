package com.yangbin1.mq.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class Consumer {

    public static void main(String[] args) throws MQClientException {

        //1.创建DefaultMQPushConsumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("demo_consumer_group");

        //2.nameSrvAddress
        consumer.setNamesrvAddr("47.101.10.100:9876");

        //设置消息拉取上限
        consumer.setConsumeMessageBatchMaxSize(2);

        //3.订阅主题
        consumer.subscribe("Topic_Demo", "*");

        //4.消息监听
        consumer.setMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                //迭代消息信息
                for (MessageExt msg : list) {
                    try {
                        //获取主题
                        String topic = msg.getTopic();
                        //获取标签
                        String tags = msg.getTags();

                        //获取消息
                        String result = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        System.out.println("Consumer消费信息---topic:" + topic + "," + "tags:" + tags + "result:" + result);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        //重试
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
                //消费成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        //5.开启consumer
        consumer.start();
    }
}

