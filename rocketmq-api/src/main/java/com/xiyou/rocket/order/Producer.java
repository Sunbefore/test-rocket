package com.xiyou.rocket.order;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * @author 92823
 * @Package：com.xiyou.rocket.simple.com.xiyou.rocket.order
 * @ClassName:
 * @Description: 顺序消息的生产者
 * @version：3.0
 * @date 2020/12/7 23:23
 * <p>
 * Company：润联科技
 */
public class Producer {
    public static void main(String[] args) throws Exception{
        try {
            // 定义一个生产者，是顺序消息
            DefaultMQProducer producer = new DefaultMQProducer("ORDER_PRODUCER_GROUP");
            producer.setNamesrvAddr("139.224.49.214:9876");
            producer.start();
            for (int i = 0; i < 10; i++) {
                // 定义orderId
                int orderId = i;
                for (int j = 0; j < 5; j++) {
                    Message message = new Message("ORDER_TOPIC",
                            "order_" + orderId,
                            "TAG" + orderId,
                            ("order_" + orderId + "_" + j).getBytes());
                    // orderId是传进来的 传导里面的Object 0里面
                    SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                        /**
                         * 根据id选择发送到哪一个队列中
                         *
                         * @param list
                         * @param message
                         * @param o
                         * @return
                         */
                        @Override
                        public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                            System.out.println("broker中队列的数量: " + list.size());
                            Integer id = (Integer) o;
                            // id取余队列长度
                            int index = id % list.size();
                            // 表示发送到第几个队列中
                            return list.get(index);
                        }
                    }, orderId);
                    System.out.println("ok");
                }
            }
            producer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
