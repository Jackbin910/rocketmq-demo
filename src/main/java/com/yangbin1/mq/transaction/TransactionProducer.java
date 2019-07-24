package com.yangbin1.mq.transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;

/**
 * @ClassName: OrderProducer
 * @Auther: yangbin1
 * @Date: 2019/7/22 15:53
 * @Description:
 */
public class TransactionProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException {

        //1.创建TransactionProducer
        TransactionMQProducer producer = new TransactionMQProducer("demo_producer_transaction_group");

        //2.nameSrvAddress
        producer.setNamesrvAddr("47.101.10.100:9876");

        //指定消息监听对象，用于执行本地事务和消息回查
        TransactionListener transactionListener = new TransactionListenerImpl();
        producer.setTransactionListener(transactionListener);

        //线程池
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });

        producer.setExecutorService(executorService);

        //3.开启producer
        producer.start();

        //5.发送消息
        // 第一个参数发送的消息，
        // 第二个参数指定队列
        // 指定队列下标
        Message message = new Message("Topic_Transaction_Demo", "Tags", "Keys", ("hello!-transaction").getBytes(RemotingHelper.DEFAULT_CHARSET));

        // 发送事务消息
        TransactionSendResult result = producer.sendMessageInTransaction(message, "hello-transaction");

        System.out.println(result);

        //关闭
        producer.shutdown();

    }
}
