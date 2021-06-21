package org.bigwinner.canal.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.bigwinner.protobuf.CanalMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author: bigwinner
 * @date: 2021/6/20 下午12:08
 * @version: 1.0.0
 * @description: Canal客户端工具类
 */
public class ClientCanalUtils {
    private final static Logger LOGGER = LoggerFactory.getLogger(ClientCanalUtils.class);
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String SEP = SystemUtils.LINE_SEPARATOR;
    private static String context_format = null;
    private static String row_format = null;
    private static String transaction_format = null;


    public ClientCanalUtils() {
        context_format = SEP + "****************************************************" + SEP;
        context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP;
        context_format += "* Start : [{}] " + SEP;
        context_format += "* End : [{}] " + SEP;
        context_format += "****************************************************" + SEP;

        row_format = SEP
                + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {}({}) , gtid : ({}) , delay : {} ms"
                + SEP;

        transaction_format = SEP
                + "================> binlog[{}:{}] , dbInfo[{}:{}] , executeTime : {}({}) , gtid : ({}) , delay : {}ms"
                + SEP;
    }
    
    /**
     * 打印canal消息对象：
     * Entry
     *     Header
     *         logfileName [binlog文件名]
     *         logfileOffset [binlog position]
     *         executeTime [binlog里记录变更发生的时间戳,精确到秒]
     *         schemaName
     *         tableName
     *         eventType [insert/update/delete类型]
     *     EntryType   [事务头BEGIN/事务尾END/数据ROWDATA]
     *     StoreValue  [byte数据,可展开，对应的类型为RowChange]
     * RowChange
     *     isDdl       [是否是ddl变更操作，比如create table/drop table]
     *     sql         [具体的ddl sql]
     * rowDatas    [具体insert/update/delete的变更数据，可为多条，1个binlog event事件可对应多条变更，比如批处理]
     *     beforeColumns [Column类型的数组，变更前的数据字段]
     *     afterColumns [Column类型的数组，变更后的数据字段]
     *     Column
     *     index
     *     sqlType     [jdbc type]
     *     name        [column name]
     *     isKey       [是否为主键]
     *     updated     [是否发生过变更]
     *     isNull      [值是否为null]
     *     value       [具体的内容，注意为string文本]
     * @param entries
     */
    protected void printEntries(List<CanalEntry.Entry> entries) {
        for (CanalEntry.Entry entry : entries) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = System.currentTimeMillis() - executeTime;
            Date date = new Date(entry.getHeader().getExecuteTime());
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            // 事务开始、结束
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
                    CanalEntry.TransactionBegin begin = null;
                    try {
                        begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务头信息，执行的线程id，事务耗时
                    LOGGER.info(transaction_format,
                            new Object[] { entry.getHeader().getLogfileName(),
                                    String.valueOf(entry.getHeader().getLogfileOffset()),
                                    entry.getHeader().getSchemaName(),
                                    entry.getHeader().getTableName(),
                                    String.valueOf(entry.getHeader().getExecuteTime()), simpleDateFormat.format(date),
                                    entry.getHeader().getGtid(), String.valueOf(delayTime) });
                    LOGGER.info(" BEGIN ----> Thread id: {}", begin.getThreadId());
                    printXAInfo(begin.getPropsList());
                } else if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {

                    CanalEntry.TransactionEnd end = null;
                    try {
                        end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    LOGGER.info("----------------\n");
                    LOGGER.info(" END ----> transaction id: {}", end.getTransactionId());
                    printXAInfo(end.getPropsList());
                    LOGGER.info(transaction_format,
                            new Object[] { entry.getHeader().getLogfileName(),
                                    String.valueOf(entry.getHeader().getLogfileOffset()),
                                    entry.getHeader().getSchemaName(),
                                    entry.getHeader().getTableName(),
                                    String.valueOf(entry.getHeader().getExecuteTime()), simpleDateFormat.format(date),
                                    entry.getHeader().getGtid(), String.valueOf(delayTime) });
                }
                continue;
            } else if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                CanalEntry.RowChange rowChage = null;
                try {
                    rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                CanalEntry.EventType eventType = rowChage.getEventType();

                LOGGER.info(row_format,
                        new Object[] { entry.getHeader().getLogfileName(),
                                String.valueOf(entry.getHeader().getLogfileOffset()), entry.getHeader().getSchemaName(),
                                entry.getHeader().getTableName(), eventType,
                                String.valueOf(entry.getHeader().getExecuteTime()), simpleDateFormat.format(date),
                                entry.getHeader().getGtid(), String.valueOf(delayTime) });

                if (eventType == CanalEntry.EventType.QUERY || rowChage.getIsDdl()) {
                    LOGGER.info("ddl : " + rowChage.getIsDdl() + " ,  sql ----> " + rowChage.getSql() + SEP);
                    continue;
                }

                printXAInfo(rowChage.getPropsList());
                for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                    if (eventType == CanalEntry.EventType.DELETE) {
                        printColumn(rowData.getBeforeColumnsList());
                    } else if (eventType == CanalEntry.EventType.INSERT) {
                        printColumn(rowData.getAfterColumnsList());
                    } else {
                        printColumn(rowData.getAfterColumnsList());
                    }
                }
            }
        }
    }

    protected void printXAInfo(List<CanalEntry.Pair> pairs) {
        if (pairs == null) {
            return;
        }
        String xaType = null;
        String xaXid = null;
        for (CanalEntry.Pair pair : pairs) {
            String key = pair.getKey();
            if (StringUtils.endsWithIgnoreCase(key, "XA_TYPE")) {
                xaType = pair.getValue();
            } else if (StringUtils.endsWithIgnoreCase(key, "XA_XID")) {
                xaXid = pair.getValue();
            }
        }

        if (xaType != null && xaXid != null) {
            LOGGER.info(" ------> " + xaType + " " + xaXid);
        }
    }

    /**
     * 构建偏移量信息
     * @param entry
     * @return
     */
    protected String buildPositionForDump(CanalEntry.Entry entry) {
        // sql 执行时间
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        // 格式化执行日期
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);

        String position = entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":"
                + entry.getHeader().getExecuteTime() + "(" + format.format(date) + ")";
        if (StringUtils.isNotEmpty(entry.getHeader().getGtid())) {
            position += " gtid(" + entry.getHeader().getGtid() + ")";
        }
        return position;
    }

    /**
     * 打印列信息
     * @param columns
     */
    protected void printColumn(List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            StringBuilder builder = new StringBuilder();
            try {
                if (StringUtils.containsIgnoreCase(column.getMysqlType(), "BLOB")
                        || StringUtils.containsIgnoreCase(column.getMysqlType(), "BINARY")) {
                    // get value bytes
                    builder.append(column.getName() + " : "
                            + new String(column.getValue().getBytes("ISO-8859-1"), "UTF-8"));
                } else {
                    builder.append(column.getName() + " : " + column.getValue());
                }
            } catch (UnsupportedEncodingException e) {
            }
            builder.append("    type=" + column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=" + column.getUpdated());
            }
            builder.append(SEP);
            LOGGER.info(builder.toString());
        }
    }

    protected void printSummary(Message message, long batchId, int size) {
        long memsize = message.getEntries().stream().mapToLong(entry -> entry.getHeader().getEventLength()).sum();
        String startPosition = null;
        String endPosition = null;
        if (!CollectionUtils.isEmpty(message.getEntries())) {
            startPosition = buildPositionForDump(message.getEntries().get(0));
            endPosition = buildPositionForDump(message.getEntries().get(message.getEntries().size() - 1));
        }

        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        LOGGER.info(context_format, new Object[] { batchId, size, memsize, format.format(new Date()), startPosition,
                endPosition });
    }

    /**
     * 使用protoBuf序列化canal消息
     * @param entries
     * @return
     */
    public byte[] canalMsgToProto(List<CanalEntry.Entry> entries, CanalMsg.RowData.Builder rowDataBuilder) {
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType().equals(CanalEntry.EntryType.TRANSACTIONBEGIN) || entry.getEntryType().equals(CanalEntry.EntryType.TRANSACTIONEND)) {
                continue;
            } else if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                CanalEntry.Header entryHeader = entry.getHeader();
                // 1. 赋值行数据
                // binlog日志文件名称
                rowDataBuilder.setLogfilename(entryHeader.getLogfileName());
                // binlog日志偏移量
                rowDataBuilder.setLogfileoffset(entryHeader.getLogfileOffset());
                // sql操作类型
                rowDataBuilder.setEventType(entryHeader.getEventType().toString().toLowerCase());
                // sql执行时间戳
                rowDataBuilder.setExecuteTime(entryHeader.getExecuteTime());
                // 数据库名称
                rowDataBuilder.setSchemaName(entryHeader.getSchemaName());
                // 表名称
                rowDataBuilder.setTableName(entryHeader.getTableName());

                // 2. 赋值列数据
                try {
                    CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    rowChange.getRowDatasList().forEach(rowData -> {
                        if (entryHeader.getEventType().toString().toLowerCase().equals("insert")
                                || entryHeader.getEventType().toString().toLowerCase().equals("update")) {
                            rowData.getAfterColumnsList().forEach(column -> {
                                rowDataBuilder.putColumns(column.getName(), column.getValue());
                            });
                        } else if (entryHeader.getEventType().toString().toLowerCase().equals("delete")) {
                            rowData.getBeforeColumnsList().forEach(column -> {
                                rowDataBuilder.putColumns(column.getName(), column.getValue());
                            });
                        }
                    });
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
            }
        }
        // 3. 序列化canal消息
        return rowDataBuilder.build().toByteArray();
    }

    /**
     * Canal消息转换为json对象
     * @param entries
     */
    public String canalMsgToJson(List<CanalEntry.Entry> entries) {
        // 封装行数据
        Map<String, Object> rowDataMap = Maps.newHashMap();
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType().equals(CanalEntry.EntryType.TRANSACTIONBEGIN) || entry.getEntryType().equals(CanalEntry.EntryType.TRANSACTIONEND)) {
                continue;
            }
            CanalEntry.Header entryHeader = entry.getHeader();
            // 表名
            String tableName = entryHeader.getTableName();
            // binlog文件名
            String logFileName = entryHeader.getLogfileName();
            // 数据库名
            String schemaName = entryHeader.getSchemaName();
            // binlog日志偏移量
            long logfileOffset = entryHeader.getLogfileOffset();
            // sql执行的时间戳
            long executeTime = entryHeader.getExecuteTime();
            // sql事件类型
            String eventType = entryHeader.getEventType().toString().toLowerCase();

            rowDataMap.put("tableName", tableName);
            rowDataMap.put("logFileName", logFileName);
            rowDataMap.put("schemaName", schemaName);
            rowDataMap.put("logfileOffset", logfileOffset);
            rowDataMap.put("executeTime", executeTime);
            rowDataMap.put("eventType", eventType);

            // 封装列数据
            Map<String, Object> columnDataMap = Maps.newHashMap();
            try {
                CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                rowDatasList.forEach(rowData -> {
                    if (eventType.equals("insert") || eventType.equals("update")) {
                        // 封装插入和更改操作的行记录的所有字段
                        List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                        afterColumnsList.forEach(column -> {
                            columnDataMap.put(column.getName(), column.getValue());
                        });
                    } else if (eventType.equals("delete")) {
                        // 封装删除操作的行记录的所有字段
                        List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                        beforeColumnsList.forEach(column -> {
                            columnDataMap.put(column.getName(), column.getValue());
                        });
                    }
                });
                rowDataMap.put("columns", columnDataMap);
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
        return JSON.toJSONString(rowDataMap);
    }
}
