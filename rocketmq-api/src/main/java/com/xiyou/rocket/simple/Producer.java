package com.xiyou.rocket.simple;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

/**
 * @author 92823
 * @Package：com.xiyou.rocket.simple
 * @ClassName:
 * @Description: 简单的发送
 * @version：3.0
 * @date 2020/12/7 21:26
 * <p>
 * Company：润联科技
 */
public class Producer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
        // 设置发送的broker地址
        // 发送给了四个队列
        producer.setNamesrvAddr("139.224.49.214:9876");
        // 启动
        producer.start();
        for (int i = 0; i < 20; i++) {
            try {
                Message message = new Message("TopicTestSimple", "TagA", "OrderID188", "Hello Simple".getBytes());
                // 同步发送消息 消息会发送给集群中的一个指定的Broker
                producer.sendOneway(message);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // 关闭消息的发送端
        producer.shutdown();
    }
}
