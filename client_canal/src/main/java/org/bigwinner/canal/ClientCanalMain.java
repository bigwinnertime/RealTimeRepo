package org.bigwinner.canal;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.InvalidProtocolBufferException;
import org.bigwinner.canal.utils.ClientCanalUtils;
import org.bigwinner.commons.utils.ConfigFileUtils;
import org.bigwinner.protobuf.CanalMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * @author: bigwinner
 * @date: 2021/6/20 下午12:07
 * @version: 1.0.0
 * @description: Canal客户端主类
 */
public class ClientCanalMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientCanalMain.class);
    private static ConfigFileUtils configFileUtils;
    private CanalConnector connector;
    static {
        configFileUtils = new ConfigFileUtils("canal.properties");
    }

    private static final String CANAL_HOSTS = configFileUtils.getValueFromProperties(ConstansValue.CANAL_SERVER_HOSTS);
    private static final int CANAL_PORT = Integer.parseInt(configFileUtils.getValueFromProperties(ConstansValue.CANAL_SERVER_PORT));
    private static final String CANAL_USER = configFileUtils.getValueFromProperties(ConstansValue.CANAL_SERVER_USER);
    private static final String CANAL_PASS = configFileUtils.getValueFromProperties(ConstansValue.CANAL_SERVER_PASS);
    private static final String CANAL_DESTINATION = configFileUtils.getValueFromProperties(ConstansValue.CANAL_SERVER_DESTINATION);
    private static final int CANAL_ACK_BATCHSIZE = Integer.parseInt(configFileUtils.getValueFromProperties(ConstansValue.CANAL_ACK_BATCHSIZE));

    public static void main(String[] args) {
        ClientCanalMain canalMain = new ClientCanalMain();
        // 1. 初始化连接canal
        canalMain.initCanalClient();
        // 2. 处理数据
        canalMain.process();
    }

    private void process() {
        // 1. 连接
        connector.connect();
        // 2. 订阅数据库
        connector.subscribe("bigwinner_data.*");
        // 3. 初始化protobuf构造器
        CanalMsg.RowData.Builder rowDataBuilder = CanalMsg.RowData.newBuilder();
        // 4. 处理数据
        try {
            while (true) {
                // 4.1 按照批次大小批量拉取canal消息
                Message message = connector.getWithoutAck(CANAL_ACK_BATCHSIZE);
                // 4.2 获取该批次的ID
                long batchId = message.getId();
                // 4.3 获取该批次的消息大小
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    // noop
                } else {
                    ClientCanalUtils canalUtils = new ClientCanalUtils();
                    // 4.4 处理消息
                    // canal消息转换为json
                    String msg = canalUtils.canalMsgToJson(message.getEntries());
                    LOGGER.warn("拉取到的canal消息为：{}", msg);

                    // canal消息序列化为protobuf格式消息
                    byte[] msgBytes = canalUtils.canalMsgToProto(message.getEntries(), rowDataBuilder);
                    // 反序列化protobuf消息
                    CanalMsg.RowData rowData = null;
                    try {
                        rowData = CanalMsg.RowData.parseFrom(msgBytes);
                    } catch (InvalidProtocolBufferException e) {
                        LOGGER.error("反序列化失败：{}", e);
                    }
                    LOGGER.warn("获取到的binlog操作类型为：{}, 数据库名称为：{}, 所有的列信息为：{}", rowData.getEventType(),
                            rowData.getSchemaName(), JSON.toJSONString(rowData.getColumnsMap()));

                }
                // 4.5 ack消息
                connector.ack(batchId);
            }
        } catch (CanalClientException e) {
            LOGGER.error("canal消息处理失败: {}", e);
        } finally {
            // 关闭连接
            closeConnect();
        }
    }

    private void initCanalClient() {
        connector = CanalConnectors.newSingleConnector(new InetSocketAddress(CANAL_HOSTS, CANAL_PORT), CANAL_DESTINATION,
                CANAL_USER, CANAL_PASS);
    }

    private void closeConnect() {
        if (connector != null) {
            connector.disconnect();
        }
    }
}
