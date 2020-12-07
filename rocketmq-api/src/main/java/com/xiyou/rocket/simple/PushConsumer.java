package com.xiyou.rocket.simple;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author 92823
 * @Package：com.xiyou.rocket.simple
 * @ClassName:
 * @Description: 推模式的消费 指的是broker推消息给消费者
 * @version：3.0
 * @date 2020/12/7 22:29
 * <p>
 * Company：润联科技
 */
public class PushConsumer {
    public static void main(String[] args) throws Exception{
        // 定义一个推模式的消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("CID_JODIE_2");
        consumer.setNamesrvAddr("139.224.49.214:9876");
        // 订阅指定主题的消息，tag是* 表示接受所有的标签
        consumer.subscribe("TopicTestSimple", "*");
        // 从第一个offset开始接受消息
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 时间戳
        consumer.setConsumeTimestamp("20201207225240");
        // 注册监听
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            /**
             * 接收到消息之后 会调用这个方法
             *
             * @param list
             * @param context
             * @return
             */
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                for (MessageExt messageExt : list) {
                    System.out.println("接收到的消息是: " + new String(messageExt.getBody()));
                }
                // System.out.println("接收到的消息: " + JSONObject.toJSONString(list));
                System.out.println("context: " + context);
                // 表示接受成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("Consume Started");
    }
}
