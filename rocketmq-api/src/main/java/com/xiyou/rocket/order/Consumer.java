package com.xiyou.rocket.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author 92823
 * @Package：com.xiyou.rocket.order
 * @ClassName:
 * @Description: 顺序消息的消费者
 * @version：3.0
 * @date 2020/12/7 23:24
 * <p>
 * Company：润联科技
 */
public class Consumer {

    public static void main(String[] args) throws Exception{
        // 推模式
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ORDER_CONSUMER_GROUP");
        consumer.setNamesrvAddr("139.224.49.214:9876");
        // 设置偏移量以0为主
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 订阅指定的主题，以及Tag
        consumer.subscribe("ORDER_TOPIC", "*");
        // 订阅消息
        // 消费者从一个队列中取，取完一个队列之后，再去取下一个队列的消息
        // 之前是纵向的，一个队列取一个，现在是一个队列取完再取另一个
        consumer.registerMessageListener(new MessageListenerOrderly() {
            // 这里的内部类是带有排序的
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                // 自动确认
                consumeOrderlyContext.setAutoCommit(true);
                for (MessageExt msg : list) {
                    byte[] body = msg.getBody();
                    System.out.println("收到的消息: " + new String(body));
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
        System.out.println("Consumer started");
    }
}
