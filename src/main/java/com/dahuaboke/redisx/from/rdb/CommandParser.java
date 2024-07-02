package com.dahuaboke.redisx.from.rdb;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.from.rdb.stream.Stream;
import com.dahuaboke.redisx.from.rdb.zset.ZSetEntry;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 2024/5/22 15:40
 * auth: dahua
 * desc:
 */
public class CommandParser {

    private static final Logger logger = LoggerFactory.getLogger(CommandParser.class);
    private Map<Integer, Type> typeMap = new HashMap() {{
        put(0x00 & 0xff, Type.STRING);
        put(0x01 & 0xff, Type.LIST);
        put(0x02 & 0xff, Type.SET);
        put(0x03 & 0xff, Type.ZSET);
        put(0x04 & 0xff, Type.HASH);
        put(0x05 & 0xff, Type.ZSET);
        put(0x06 & 0xff, Type.MOUDULE);
        put(0x07 & 0xff, Type.MOUDULE);
        put(0x09 & 0xff, Type.HASH);
        put(0x0a & 0xff, Type.LIST);
        put(0x0b & 0xff, Type.SET);
        put(0x0c & 0xff, Type.ZSET);
        put(0x0d & 0xff, Type.HASH);
        put(0x0e & 0xff, Type.LIST);
        put(0x0f & 0xff, Type.STREAM);
        put(0x10 & 0xff, Type.HASH);
        put(0x11 & 0xff, Type.ZSET);
        put(0x12 & 0xff, Type.LIST);
        put(0x13 & 0xff, Type.STREAM);
        put(0x14 & 0xff, Type.SET);
        put(0x15 & 0xff, Type.STREAM);
    }};

    private enum Type {
        STRING,
        LIST,
        SET,
        ZSET,
        HASH,
        MOUDULE,
        STREAM,
        FUNCTION;
    }

    public List<ArrayRedisMessage> parser(RdbHeader rdbHeader) {
        List<ArrayRedisMessage> result = new LinkedList();
        if (rdbHeader.getFunction() != null && rdbHeader.getFunction().size() > 0) {
            function(result, rdbHeader.getFunction());
        }
        return result;
    }

    public List<ArrayRedisMessage> parser(RdbData rdbData) {
        List<ArrayRedisMessage> result = new LinkedList();
        switch (typeMap.get(rdbData.getRdbType())) {
            case STRING:
                string(result, rdbData.getKey(), (byte[]) rdbData.getValue());
                break;
            case LIST:
                list(result, rdbData.getKey(), (List<byte[]>) rdbData.getValue());
                break;
            case SET:
                set(result, rdbData.getKey(), (Set<byte[]>) rdbData.getValue());
                break;
            case ZSET:
                zet(result, rdbData.getKey(), (Set<ZSetEntry>) rdbData.getValue());
                break;
            case HASH:
                hash(result, rdbData.getKey(), (Map<byte[], byte[]>) rdbData.getValue());
                break;
            case MOUDULE:
                moudule(result, rdbData.getKey(), rdbData.getValue());
                break;
            case STREAM:
                stream(result, rdbData.getKey(), (Stream) rdbData.getValue());
                break;
            default:
                throw new IllegalArgumentException("Rdb type error");
        }
        long expireTime = rdbData.getExpireTime();
        long lastTime = expireTime - System.currentTimeMillis();
        ExpiredType expiredType = rdbData.getExpiredType();
        if (ExpiredType.NONE != expiredType) {
            List<RedisMessage> fullList = new ArrayList<>();
            fullList.add(createMassage("expire"));
            fullList.add(createMassage(rdbData.getKey()));
            if (ExpiredType.SECOND == expiredType) {
                fullList.add(createMassage(lastTime + ""));
            } else if (ExpiredType.MS == expiredType) {
                fullList.add(createMassage(lastTime/1000 + ""));
            } else {
                throw new IllegalArgumentException("Rdb type error");
            }
            result.add(new ArrayRedisMessage(fullList));
        }
        return result;
    }

    private void string(List<ArrayRedisMessage> list, byte[] key, byte[] value) {
        List<RedisMessage> fullList = new ArrayList<>();
        fullList.add(createMassage("SET"));
        fullList.add(createMassage(key));
        fullList.add(createMassage(value));
        list.add(new ArrayRedisMessage(fullList));
    }

    private void list(List<ArrayRedisMessage> list, byte[] key, List<byte[]> value) {
        List<RedisMessage> fullList = new ArrayList<>();
        fullList.add(createMassage("LPUSH"));
        fullList.add(createMassage(key));
        for (byte[] bytes : value) {
            fullList.add(createMassage(bytes));
        }
        list.add(new ArrayRedisMessage(fullList));
    }

    private void set(List<ArrayRedisMessage> list, byte[] key, Set<byte[]> value) {
        List<RedisMessage> fullList = new ArrayList<>();
        fullList.add(createMassage("SADD"));
        fullList.add(createMassage(key));
        for (byte[] bytes : value) {
            fullList.add(createMassage(bytes));
        }
        list.add(new ArrayRedisMessage(fullList));
    }

    private void zet(List<ArrayRedisMessage> list, byte[] key, Set<ZSetEntry> value) {
        List<RedisMessage> fullList = new ArrayList<>();
        fullList.add(createMassage("ZADD"));
        fullList.add(createMassage(key));
        for (ZSetEntry zSetEntry : value) {
            String score = String.valueOf(zSetEntry.getScore());
            byte[] element = zSetEntry.getElement();
            fullList.add(createMassage(score));
            fullList.add(createMassage(element));
        }
        list.add(new ArrayRedisMessage(fullList));
    }

    private void hash(List<ArrayRedisMessage> list, byte[] key, Map<byte[], byte[]> value) {
        List<RedisMessage> fullList = new ArrayList<>();
        fullList.add(createMassage("HSET"));
        fullList.add(createMassage(key));
        for (Map.Entry<byte[], byte[]> kAbdV : value.entrySet()) {
            byte[] key1 = kAbdV.getKey();
            byte[] value1 = kAbdV.getValue();
            fullList.add(createMassage(key1));
            fullList.add(createMassage(value1));
        }
        list.add(new ArrayRedisMessage(fullList));
    }

    private void stream(List<ArrayRedisMessage> list, byte[] key, Stream value) {
        String streamName = new String(key);
        if (!value.getEntries().isEmpty()) {
            for (Map.Entry<Stream.ID, Stream.Entry> kandv : value.getEntries().entrySet()) {
                if (kandv.getValue().isDeleted()) {
                    continue;
                }
                List<RedisMessage> fullList = new ArrayList<>();
                fullList.add(createMassage("XADD"));
                fullList.add(createMassage(streamName));
                fullList.add(createMassage(kandv.getKey().getMs() + "-" + kandv.getKey().getSeq()));
                for (Map.Entry<byte[], byte[]> entry : kandv.getValue().getFields().entrySet()) {
                    fullList.add(createMassage(entry.getKey()));
                    fullList.add(createMassage(entry.getValue()));
                }
                list.add(new ArrayRedisMessage(fullList));
            }
        }
        if (!value.getGroups().isEmpty()) {
            for (int i = 0; i < value.getGroups().size(); i++) {
                Stream.Group group = value.getGroups().get(i);
                StringBuilder sb = new StringBuilder();
                List<RedisMessage> fullList = new ArrayList<>();
                fullList.add(createMassage("XGROUP"));
                fullList.add(createMassage("CREATE"));
                fullList.add(createMassage(streamName));
                fullList.add(createMassage(group.getName()));
                fullList.add(createMassage(group.getLastId().getMs() + "-" + group.getLastId().getSeq()));
                fullList.add(createMassage("ENTRIESREAD"));
                fullList.add(createMassage(group.getEntriesRead() + ""));
                list.add(new ArrayRedisMessage(fullList));
                if (group.getConsumers() != null && group.getConsumers().size() > 0) {
                    for (int m = 0; m < group.getConsumers().size(); m++) {
                        List<RedisMessage> fullList2 = new ArrayList<>();
                        fullList2.add(createMassage("XGROUP"));
                        fullList2.add(createMassage("CREATECONSUMER"));
                        fullList2.add(createMassage(streamName));
                        fullList2.add(createMassage(group.getName()));
                        fullList2.add(createMassage(group.getConsumers().get(m).getName()));
                        list.add(new ArrayRedisMessage(fullList2));
                    }
                }
            }
        }
    }

    private void moudule(List<ArrayRedisMessage> list, byte[] key, Object data) {

    }

    private void function(List<ArrayRedisMessage> list, List<byte[]> value) {
        if (value != null && value.size() > 0) {
            value.forEach(v -> {
                List<RedisMessage> fullList = new ArrayList<>();
                fullList.add(createMassage("FUNCTION"));
                fullList.add(createMassage("LOAD"));
                fullList.add(createMassage(v));
                list.add(new ArrayRedisMessage(fullList));
            });
        }
    }

    private FullBulkStringRedisMessage createMassage(String command){
        return createMassage("FUNCTION".getBytes());
    }

    private FullBulkStringRedisMessage createMassage(byte[] command){
        return new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes(command));
    }

}
