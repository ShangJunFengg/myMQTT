import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import org.apache.commons.lang3.StringUtils;

public class BrokerHandler extends SimpleChannelInboundHandler<MqttMessage> {
    private MqttVersion version;
    private String clientId;
    private String userName;
    private String brokerId;
    private boolean connected;
    private boolean cleanSession;
    private int keepAlive;
    private int keepAliveMax;
    private MqttPublishMessage willMessage;

    public BrokerHandler(int keepAliveMax) {

        this.keepAliveMax = keepAliveMax;
    }

    @Override
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {

        if (msg.decoderResult().isFailure()) {

            Throwable cause = msg.decoderResult().cause();

            if (cause instanceof MqttUnacceptableProtocolVersionException) {

                BrokerSessionHelper.sendMessage(
                        ctx,
                        MqttMessageFactory.newMessage(
                                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, false),
                                null),
                        "INVALID",
                        null,
                        true);

            } else if (cause instanceof MqttIdentifierRejectedException) {

                BrokerSessionHelper.sendMessage(
                        ctx,
                        MqttMessageFactory.newMessage(
                                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, false),
                                null),
                        "INVALID",
                        null,
                        true);
            }

            ctx.close();

            return;
        }

        switch (msg.fixedHeader().messageType()) {
            case CONNECT:
                onConnect(ctx, (MqttConnectMessage) msg);
                break;
            case PUBLISH:
                onPublish(ctx, (MqttPublishMessage) msg);
                break;
            case PUBACK:
                onPubAck(ctx, msg);
                break;
            case PUBREC:
                onPubRec(ctx, msg);
                break;
            case PUBREL:
                onPubRel(ctx, msg);
                break;
            case PUBCOMP:
                onPubComp(ctx, msg);
                break;
            case SUBSCRIBE:
                onSubscribe(ctx, (MqttSubscribeMessage) msg);
                break;
            case UNSUBSCRIBE:
                onUnsubscribe(ctx, (MqttUnsubscribeMessage) msg);
                break;
            case PINGREQ:
                onPingReq(ctx);
                break;
            case DISCONNECT:
                onDisconnect(ctx);
                break;
        }

    }

    private void onConnect(ChannelHandlerContext ctx, MqttConnectMessage msg) {

        this.version = MqttVersion.fromProtocolNameAndLevel(msg.variableHeader().name(), (byte) msg.variableHeader().version());
        this.clientId = msg.payload().clientIdentifier();
        this.cleanSession = msg.variableHeader().isCleanSession();

        if (msg.variableHeader().keepAliveTimeSeconds() > 0 && msg.variableHeader().keepAliveTimeSeconds() <= this.keepAliveMax) {
            this.keepAlive = msg.variableHeader().keepAliveTimeSeconds();
        }

        //MQTT 3.1之后可能存在为空的客户ID。所以要进行处理。如果客户ID是空，而且还在保存处理相关的信息。这样子是不行。
        //必须有客户ID我们才能存保相关信息。
        if (StringUtils.isBlank(this.clientId)) {
            if (!this.cleanSession) {

                BrokerSessionHelper.sendMessage(
                        ctx,
                        MqttMessageFactory.newMessage(
                                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, false),
                                null),
                        "INVALID",
                        null,
                        true);

                ctx.close();

                return;

            } else {
                this.clientId =  java.util.UUID.randomUUID().toString();
            }
        }

        //有可能发送俩次的连接包。如果已经存在连接就是关闭当前的连接。
        if (this.connected) {
            ctx.close();
            return;
        }


        boolean userNameFlag = msg.variableHeader().hasUserName();
        boolean passwordFlag = msg.variableHeader().hasPassword();
        this.userName = msg.payload().userName();

        String password = "" ;
        if( msg.payload().passwordInBytes() != null  && msg.payload().passwordInBytes().length > 0)
            password =   new String(msg.payload().passwordInBytes());

        boolean mistake = false;

        //如果有用户名标示，那么就必须有密码标示。
        //当有用户名标的时候，用户不能为空。
        //当有密码标示的时候，密码不能为空。
        if (userNameFlag) {
            if (StringUtils.isBlank(this.userName))
                mistake = true;
        } else {
            if (StringUtils.isNotBlank(this.userName) || passwordFlag) mistake = true;
        }


        if (passwordFlag) {

            if (StringUtils.isBlank(password)) mistake = true;
        } else {
            if (StringUtils.isNotBlank(password)) mistake = true;
        }

        if (mistake) {
            BrokerSessionHelper.sendMessage(
                    ctx,
                    MqttMessageFactory.newMessage(
                            new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                            new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, false),
                            null),
                    this.clientId,
                    null,
                    true);
            ctx.close();
            return;
        }

        BrokerSessionHelper.sendMessage(
                ctx,
                MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, !this.cleanSession),
                        null),
                this.clientId,
                null,
                true);

        ChannelHandlerContext lastSession = BrokerSessionHelper.removeSession(this.clientId);
        if (lastSession != null) {
            lastSession.close();
        }

        String willTopic = msg.payload().willTopic();
        String willMessage = "";
        if(msg.payload().willMessageInBytes() != null && msg.payload().willMessageInBytes().length > 0)
            willMessage =  new String(msg.payload().willMessageInBytes());

        if (msg.variableHeader().isWillFlag() && StringUtils.isNotEmpty(willTopic) && StringUtils.isNotEmpty(willMessage)) {

            this.willMessage = (MqttPublishMessage) MqttMessageFactory.newMessage(
                    new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.valueOf(msg.variableHeader().willQos()), msg.variableHeader().isWillRetain(), 0),
                    new MqttPublishVariableHeader(willTopic, 0),
                    Unpooled.wrappedBuffer(willMessage.getBytes())
            );
        }

        this.connected = true;
        BrokerSessionHelper.saveSession(this.clientId, ctx);
    }

    private void onSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage msg) {
    }
    
    private void onUnsubscribe(ChannelHandlerContext ctx, MqttUnsubscribeMessage msg) {
    }

    private void onPingReq(ChannelHandlerContext ctx) {
    }

    private void onDisconnect(ChannelHandlerContext ctx) {

        if (!this.connected) {
            ctx.close();
            return;
        }

        BrokerSessionHelper.removeSession(this.clientId, ctx);

        this.willMessage = null;

        this.connected = false;

        ctx.close();

    }

    private void onPubComp(ChannelHandlerContext ctx, MqttMessage msg) {

    }
    
    private void onPubRel(ChannelHandlerContext ctx, MqttMessage msg) {
    }

    private void onPubRec(ChannelHandlerContext ctx, MqttMessage msg) {
    }
    
    private void onPubAck(ChannelHandlerContext ctx, MqttMessage msg) {
    }

    private void onPublish(ChannelHandlerContext ctx, MqttPublishMessage msg) {
    }
}

