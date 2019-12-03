import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;

public class MqttServer {

    public static void main(String[] args) throws Exception  {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            public void run() {
//                workerGroup.shutdownGracefully();
//                bossGroup.shutdownGracefully();
//            }
//        });


        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {

                        ChannelPipeline p = ch.pipeline();
                        p.addFirst("idleHandler", new IdleStateHandler(0, 0, 120));
                        p.addLast("encoder", MqttEncoder.INSTANCE);
                        p.addLast("decoder", new MqttDecoder());
                        p.addLast("logicHandler", new BrokerHandler(65535));

                    }
                })
                .option(ChannelOption.SO_BACKLOG, 511)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        ChannelFuture f = b.bind("0.0.0.0", 1883).sync();

        f.channel().closeFuture().sync();
    }

}
