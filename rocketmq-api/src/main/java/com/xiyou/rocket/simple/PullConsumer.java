package com.xiyou.rocket.simple;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author 92823
 * @Package：com.xiyou.rocket.simple
 * @ClassName:
 * @Description: 拉模式接收消息 指的是消费者主动拉取消息
 * @version：3.0
 * @date 2020/12/7 21:42
 * <p>
 * Company：润联科技
 */
public class PullConsumer {

    /**
     * 存放队列的偏移量
     */
    private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();

    public static void main(String[] args) throws Exception{
        // 声明一个拉模式的消费者
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("simple_pull_group_name");
        consumer.setNamesrvAddr("139.224.49.214:9876");
        consumer.start();
        // 消费指定的topic中的内容
        Set<MessageQueue> topicTestSimple = consumer.fetchSubscribeMessageQueues("TopicTestSimple");
        // 遍历拿到的队列
        for (MessageQueue messageQueue : topicTestSimple) {
            System.out.println("Consumer from the queue: " + JSONObject.toJSONString(messageQueue));
            SINGLE_MQ:
            while (true) {
                try {
                    PullResult pullResult =
                            consumer.pullBlockIfNotFound(messageQueue, null, getMessageQueueOffset(messageQueue), 32);
                    System.out.println(JSONObject.toJSONString(pullResult));
                    putMessageQueueOffset(messageQueue, pullResult.getNextBeginOffset());
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            break;
                        case NO_NEW_MSG:
                            break SINGLE_MQ;
                        case NO_MATCHED_MSG:
                            break;
                        case OFFSET_ILLEGAL:
                            break;
                        default:
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        consumer.shutdown();
    }

    /**
     * 得到队列的偏移量
     * @param messageQueue
     * @return
     */
    private static long getMessageQueueOffset(MessageQueue messageQueue) {
        Long offset = OFFSE_TABLE.get(messageQueue);
        if (offset != null) {
            return offset;
        }
        return 0;
    }

    /**
     * 赋值队列的偏移量
     *
     * @param messageQueue
     * @param offset
     */
    private static void putMessageQueueOffset(MessageQueue messageQueue, Long offset) {
        OFFSE_TABLE.put(messageQueue, offset);
    }
}
