package com.mycodefu;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.ErrorHandler;
import io.vertx.ext.web.handler.StaticHandler;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisChat {
    private static final Logger logger = LogManager.getLogger(RedisChat.class);

    private static final Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(100));

    public static void main(String[] args) {
        RedisMessageStream redisMessageSenderStream = new RedisMessageStream("Message Sender");
        WebSocketServer webSocketServer = new WebSocketServer(vertx, redisMessageSenderStream);

        Thread readerThread = new Thread (() -> {
            try (RedisMessageStream redisMessageReaderStream = new RedisMessageStream("Message Reader")) {
                while (true) {
                    List<ChatMessage> messages = redisMessageReaderStream.read();
                    if (messages.size() > 0) {
                        if (logger.isTraceEnabled()) {
                            logger.trace(String.format("Received %d messages from Redis:\n%s", messages.size(), messages));
                        }
                        for (ChatMessage message : messages) {
                            webSocketServer.broadcastMessage(message);
                        }
                    } else {
                        logger.error("Failed to read a message from Redis. An error may have occurred.");
                    }
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException();
                    }
                }
            } catch (InterruptedException | RedisCommandInterruptedException ex) {
                //ignore
            } catch (Exception e) {
                logger.error("An error occurred on the message reader thread:");
                e.printStackTrace();
            } finally {
                logger.info("Closing Redis Message Reader connection...");
            }
        }, "RedisReaderThread");
        readerThread.start();

        logger.info("Listening for connections on http://localhost:8080 and ws://localhost:8080!");


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("Closing server gracefully, signalling all web sockets to close...");
                webSocketServer.broadcastMessage(new ChatMessage(ChatMessageType.Close, "", "", "close now!"));
                Thread.sleep(5000);

                logger.info("Closing reader thread...");
                readerThread.interrupt();
                readerThread.join();

                logger.info("Closing web socket server...");
                webSocketServer.close();

                logger.info("Closing Redis Message Sender connection...");
                redisMessageSenderStream.close();

                logger.info("Gracefully closed all resources.");

            } catch (Exception e) {
                logger.error("Error shutting down redis connection:");
                e.printStackTrace();
            }
        }, "ShutdownHookThread"));
    }

    private static class WebSocketServer implements AutoCloseable {
        HttpServer httpServer;
        RedisMessageStream redisMessageStream;

        private static final String BROADCAST_CHANNEL = "BROADCAST_MESSAGES";

        public WebSocketServer(Vertx vertx, RedisMessageStream redisMessageStream) {
            this.redisMessageStream = redisMessageStream;

            Router router = Router.router(vertx);
            router.route().failureHandler(ErrorHandler.create(vertx));
            router.route().handler(StaticHandler.create().setCachingEnabled(false));

            httpServer = vertx.createHttpServer();
            httpServer.webSocketHandler(serverWebSocket -> {
                String socketId = serverWebSocket.binaryHandlerID();
                logger.trace(String.format("New connection received: %s", socketId));

                MessageConsumer<Object> messageConsumer = vertx.eventBus().consumer(BROADCAST_CHANNEL, messageObject -> {
                    ChatMessage message = ChatMessage.fromSerializedString((String) messageObject.body());
                    switch (message.messageType) {
                        case Close -> {
                            serverWebSocket.close();
                        }
                        case Broadcast -> {
                            if (!message.socketId.equals(socketId)) {
                                if (logger.isTraceEnabled()) {
                                    logger.trace(String.format("Broadcasting message to %s:\n%s", socketId, message.message));
                                }
                                serverWebSocket.writeTextMessage(message.message);
                            } else {
                                if (logger.isTraceEnabled()) {
                                    logger.trace(String.format("Ignoring message broadcast from self:\n%s", message.message));
                                }
                            }
                        }
                    }
                });

                serverWebSocket.handler(buffer -> {
                    String message = buffer.toString();
                    if (logger.isTraceEnabled()) {
                        logger.trace(String.format("Message received from web socket:\n%s", message));
                    }
                    ChatMessage addedMessage = redisMessageStream.add(new ChatMessage(serverWebSocket.binaryHandlerID(), message));
                    if (logger.isTraceEnabled()) {
                        logger.trace(String.format("Sent to redis:\n%s", addedMessage));
                    }
                });

                serverWebSocket.closeHandler(message -> {
                    logger.trace(String.format("Connection %s closed.", socketId));
                    messageConsumer.unregister();
                });
            });

            httpServer.requestHandler(router);
            httpServer.listen(8080).result();
            logger.info("Listening for web socket connections on 8080!");
        }

        @Override
        public void close() {
            httpServer.close();
        }

        public void broadcastMessage(ChatMessage message) {
            vertx.eventBus().publish(BROADCAST_CHANNEL, message.toSerializedString());
        }
    }

    private static class RedisMessageStream implements AutoCloseable {
        private final RedisClient redisClient;
        private final StatefulRedisConnection<String, String> connection;
        private String offset;

        public RedisMessageStream(String connectionName) {
            this(null, connectionName);
        }

        public RedisMessageStream(String offset, String connectionName) {
            redisClient = RedisClient.create("redis://localhost:18090/0");
            connection = redisClient.connect();
            logger.info(String.format("Connection '%s' established to Redis on localhost:18090", connectionName));

            if (offset == null) {
                add(new ChatMessage("init-head", "init"));
                ChatMessage head = readHead();
                if (head == null) {
                    logger.warn("Failed to write an read back a message to the messages stream. Redis may be having errors.");
                    this.offset = "0-0";
                } else {
                    this.offset = head.id;
                }
            } else {
                this.offset = offset;
            }
        }

        /**
         * Append a message to the Redis stream 'messages'.
         */
        public ChatMessage add(ChatMessage message) {
            //XADD messages * messageType "Broadcast" socketId "__vertx.ws.de4a22c7-e925-4c65-ac22-820a964c7041" message "Hi there"
            Map<String, String> messageMap = message.toMap();
            String id = connection.sync().xadd("messages", messageMap);
            return new ChatMessage(id, message.socketId, message.message);
        }

        public List<ChatMessage> read() {
            return read(Duration.ZERO);
        }

        /**
         * Return a set of messages from the Redis stream 'messages' (blocking for duration).
         * Intended to be called in a loop with a short duration.
         */
        public List<ChatMessage> read(Duration timeout) {
            //XREAD BLOCK 0 STREAMS messages 1631516018887-0
            XReadArgs.StreamOffset<String> streamOffset = XReadArgs.StreamOffset.from("messages", offset);
            RedisCommands<String, String> sync = connection.sync();
            sync.setTimeout(timeout);
            List<ChatMessage> messagesList = sync.xread(XReadArgs.Builder.block(timeout), streamOffset).stream().map(ChatMessage::fromStreamMessage).toList();
            if (messagesList.size() > 0) {
                this.offset = messagesList.get(messagesList.size() - 1).id;
            }
            return messagesList;
        }

        public ChatMessage readHead() {
            //XREVRANGE messages + - COUNT 1
            List<StreamMessage<String, String>> messages = connection.sync().xrevrange("messages", Range.unbounded(), Limit.from(1));
            if (messages != null && messages.size() > 0) {
                return ChatMessage.fromStreamMessage(messages.get(0));
            } else {
                return null;
            }
        }

        @Override
        public void close() {
            connection.close();
            redisClient.shutdown();
        }
    }

    private enum ChatMessageType {
        None,
        Broadcast,
        Close;

        public static ChatMessageType valueOfSafe(String messageTypeName) {
            if (messageTypeName == null || messageTypeName.length() == 0) {
                return ChatMessageType.None;
            } else {
                try {
                    return ChatMessageType.valueOf(messageTypeName);
                } catch (IllegalArgumentException e) {
                    return ChatMessageType.None;
                }
            }
        }
    }

    private static record ChatMessage(ChatMessageType messageType, String id, String socketId, String message) {
        public ChatMessage(String messageTypeName, String id, String socketId, String message) {
            this(ChatMessageType.valueOfSafe(messageTypeName), id, socketId, message);
        }

        public ChatMessage(String id, String socketId, String message) {
            this(ChatMessageType.Broadcast, id, socketId, message);
        }

        public ChatMessage(String socketId, String message) {
            this(null, socketId, message);
        }

        public String toSerializedString() {
            return String.format("%s|||%s|||%s|||%s", messageType.name(), id, socketId, message);
        }

        public static ChatMessage fromSerializedString(String ifmMessageString) {
            String[] parts = ifmMessageString.split("\\|\\|\\|");
            return new ChatMessage(ChatMessageType.valueOf(parts[0]), parts[1], parts[2], parts[3]);
        }

        public Map<String, String> toMap() {
            Map<String, String> messageMap = new HashMap<>();
            messageMap.put("messageType", messageType.name());
            messageMap.put("socketId", socketId);
            messageMap.put("message", message);
            return messageMap;
        }

        public static ChatMessage fromStreamMessage(StreamMessage<String, String> message) {
            if (message == null) {
                return null;
            }
            return new ChatMessage(message.getBody().get("messageType"), message.getId(), message.getBody().get("socketId"), message.getBody().get("message"));
        }

    }
}
