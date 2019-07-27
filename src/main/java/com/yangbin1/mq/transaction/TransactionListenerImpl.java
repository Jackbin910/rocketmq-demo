package com.yangbin1.mq.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ConcurrentHashMap;

public class TransactionListenerImpl implements TransactionListener {

    //存储事务的状态信息（key=事务id,value=当前事务状态）
    private ConcurrentHashMap<String, Integer> localTransaction = new ConcurrentHashMap<>();

    /**
     * 执行本地事务
     * @param msg
     * @param arg
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        //事务ID
        String transactionId = msg.getTransactionId();

        //0:执行中，1:本地事务执行成功，2:本地事务执行失败
        localTransaction.put(transactionId, 0);

        //业务执行，处理本地事务，service
        System.out.println("hello!---demo---transaction");

        try {
            System.out.println("正在执行本地事务----");
            Thread.sleep(120000);
            System.out.println("正在执行本地事务----成功");
            localTransaction.put(transactionId, 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            localTransaction.put(transactionId, 2);
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }

        return LocalTransactionState.COMMIT_MESSAGE;
    }

    /**
     * 消息回查
     * @param msg
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        //获取当前事务的id信息
        String transactionId = msg.getTransactionId();
        //获取事务状态
        Integer status = localTransaction.get(transactionId);
        System.out.println("消息回查------transactionId:" + transactionId + "状态:" + status);
        switch (status) {
            case 0:
                return LocalTransactionState.UNKNOW;
            case 1:
                return LocalTransactionState.COMMIT_MESSAGE;
            case 2:
                return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        return LocalTransactionState.UNKNOW;
    }
}
