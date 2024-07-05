package com.dahuaboke.redisx.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.to.ToContext;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;


/**
 * 2024/5/15 15:00
 * auth: cdl
 * desc:
 */
public class SlotInfoHandler extends SimpleChannelInboundHandler<String> {

    private static final Logger logger = LoggerFactory.getLogger(CommandEncoder.class);
    private Context context;
    private boolean hasPassword;

    public SlotInfoHandler(Context context, boolean hasPassword) {
        this.context = context;
        this.hasPassword = hasPassword;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (!hasPassword) {
            sendSlotCommand(ctx);
        }
        ctx.fireChannelActive();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        if (hasPassword) {
            if ("SLOTSEND".equals(msg)) {
                sendSlotCommand(ctx);
            } else {
                parseSlotMessage(ctx, msg);
            }
        } else {
            parseSlotMessage(ctx, msg);
        }
    }

    private void sendSlotCommand(ChannelHandlerContext ctx) {
        logger.info("Beginning send slot get command");
        Channel channel = ctx.channel();
        if (channel.isActive()) {
            //不需要去pipeline的底部，所以直接ctx.write
            ctx.writeAndFlush(Constant.GET_SLOT_COMMAND);
        }
    }

    private void parseSlotMessage(ChannelHandlerContext ctx, String msg) {
        logger.info("Beginning slot message parse");
        Pattern pattern = Pattern.compile(Constant.SLOT_REX, Pattern.DOTALL);
        if (msg != null && pattern.matcher(msg).matches()) {
            msg = msg.replace("\r", "");
            String[] arr = msg.split("\n");
            if (arr.length != 0) {
                Map<String, SlotInfo> map = new HashMap<>();
                Arrays.stream(arr).forEach(s -> {
                    SlotInfo slotInfo = new SlotInfo(s);
                    map.put(slotInfo.getId(), slotInfo);
                });
                map.forEach((k, v) -> {
                    if (context instanceof FromContext) {
                        FromContext fromContext = (FromContext) context;
                        if (fromContext.getHost().equals(v.getIp()) &&
                                fromContext.getPort() == v.getPort()) {
                            fromContext.setSlotInfo(v);
                            if ("master".equals(v.getFlags())) {
                                fromContext.setSlotBegin(v.getSlotStart());
                                fromContext.setSlotEnd(v.getSlotEnd());
                            } else {
                                fromContext.setSlotBegin(map.get(v.getMasterId()).getSlotStart());
                                fromContext.setSlotEnd(map.get(v.getMasterId()).getSlotEnd());
                            }
                            ctx.pipeline().remove(this);
                            return;
                        }
                    } else if (context instanceof ToContext) {
                        ToContext toContext = (ToContext) context;
                        if (toContext.getHost().equals(v.getIp()) &&
                                toContext.getPort() == v.getPort()) {
                            toContext.setSlotInfo(v);
                            if ("master".equals(v.getFlags())) {
                                toContext.setSlotBegin(v.getSlotStart());
                                toContext.setSlotEnd(v.getSlotEnd());
                            } else {
                                toContext.setSlotBegin(map.get(v.getMasterId()).getSlotStart());
                                toContext.setSlotEnd(map.get(v.getMasterId()).getSlotEnd());
                            }
                            ctx.pipeline().remove(this);
                            return;
                        }
                    }
                });
            }
        }
    }



    public class SlotInfo {
        //节点编号，40位随机字符串
        private String id;
        //节点IP
        private String ip;
        //节点端口
        private int port;
        //待确定作用
        private String nodeNum;
        //节点类型 master s
        private String flags;
        //主节点Id，只有当前节点是从节点才有值
        private String masterId;
        //最近一次发送ping的unix毫秒时间戳，0代表没有发送过
        private long pingSend;
        //最近一次收到pong的unix毫秒时间戳
        private long pingRecv;
        //该节点或其master节点的epoch值。每次故障转移都会生成一个新的，唯一的，递增的epoch值。若多个节点竞争相同的slot，epoch值大的获胜
        private int configEpoch;
        //节点和集群总线间的连接状态，可以是connected或disconnected
        private String linkState;
        //槽起始值
        private int slotStart;
        //槽终止值
        private int slotEnd;

        public SlotInfo(String msg) {
            String[] arr = msg.split(" ");
            this.id = arr[0];
            int a = arr[1].indexOf(":");
            int b = arr[1].indexOf("@");
            this.ip = arr[1].substring(0, a);
            if (b == -1) {
                port = Integer.parseInt(arr[1].substring(a + 1));
            } else {
                port = Integer.parseInt(arr[1].substring(a + 1, b));
                nodeNum = arr[1].substring(b + 1);
            }
            this.flags = arr[2].indexOf(",") == -1 ? arr[2] : arr[2].split(",")[1];
            this.masterId = arr[3];
            this.pingSend = Long.parseLong(arr[4]);
            this.pingRecv = Long.parseLong(arr[5]);
            this.configEpoch = Integer.parseInt(arr[6]);
            this.linkState = arr[7];
            if (arr.length > 8) {
                String[] slots = arr[8].split("-");
                this.slotStart = Integer.parseInt(slots[0]);
                this.slotEnd = Integer.parseInt(slots[1]);
            }
        }

        public String getIp() {
            return ip;
        }

        public int getPort() {
            return port;
        }

        public int getSlotStart() {
            return slotStart;
        }

        public int getSlotEnd() {
            return slotEnd;
        }

        public String getId() {
            return id;
        }

        public String getMasterId() {
            return masterId;
        }

        public String getFlags() {
            return flags;
        }

        public void setSlotStart(int slotStart) {
            this.slotStart = slotStart;
        }

        public void setSlotEnd(int slotEnd) {
            this.slotEnd = slotEnd;
        }
    }
}
