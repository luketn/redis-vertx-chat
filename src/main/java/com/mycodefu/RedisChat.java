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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class RedisChat {
    private static final Logger logger = LogManager.getLogger(RedisChat.class);

    private static final Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(100));

    public static void main(String[] args) {
        RedisMessageStream redisMessageSenderStream = new RedisMessageStream("Message Sender");
        WebSocketServer webSocketServer = new WebSocketServer(vertx, redisMessageSenderStream);

        Thread readerThread = new Thread(() -> {
            try (RedisMessageStream redisMessageReaderStream = new RedisMessageStream("Message Reader")) {
                while (true) {
                    List<ChatMessage> messages = redisMessageReaderStream.read();
                    if (messages.size() > 0) {
                        if (logger.isTraceEnabled()) {
                            logger.trace(String.format("Received %d messages from Redis:\n%s", messages.size(), messages));
                        }
                        for (ChatMessage message : messages) {
                            switch (message.messageType) {
                                case Broadcast: {
                                    webSocketServer.broadcastMessage(message);
                                    break;
                                }
                                case Direct: {
                                    webSocketServer.directMessage(message);
                                    break;
                                }
                            }
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

        //This shutdown hook will run _after_ vert.x's installed shutdown hook, meaning the web socket server will have been shut down along with all of its sockets by the time this runs.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("Closing reader thread...");
                readerThread.interrupt();
                readerThread.join();

                logger.info("Closing web socket server...");
                webSocketServer.close();

                logger.info("Shutting down Vert.x...");
                vertx.close().result();

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
        public static final String IDENTIFY_MESSAGE_PREFIX = "Identify:";
        private final HttpServer httpServer;

        private static final String BROADCAST_CHANNEL = "BROADCAST_MESSAGE";

        public WebSocketServer(Vertx vertx, RedisMessageStream redisMessageStream) {
            Router router = Router.router(vertx);
            router.route().failureHandler(ErrorHandler.create(vertx));
            router.route().handler(StaticHandler.create().setCachingEnabled(false));

            httpServer = vertx.createHttpServer();
            httpServer.webSocketHandler(serverWebSocket -> {
                final String socketId = serverWebSocket.binaryHandlerID();
                final AtomicReference<String> identity = new AtomicReference<>("");

                logger.trace(String.format("New connection received: %s", socketId));

                MessageConsumer<Object> messageConsumer = vertx.eventBus().consumer(BROADCAST_CHANNEL, messageObject -> {
                    ChatMessage message = ChatMessage.fromSerializedString((String) messageObject.body());
                    switch (message.messageType) {
                        case Broadcast: {
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
                            break;
                        }
                    }
                });

                serverWebSocket.handler(buffer -> {
                    String message = buffer.toString();
                    if (logger.isTraceEnabled()) {
                        logger.trace(String.format("Message received from web socket:\n%s", message));
                    }
                    if (message.startsWith(IDENTIFY_MESSAGE_PREFIX) && message.length() > IDENTIFY_MESSAGE_PREFIX.length() && identity.get().equals("")) {
                        String identityValue = message.substring(IDENTIFY_MESSAGE_PREFIX.length());
                        identity.set(identityValue);
                        vertx.eventBus().consumer(identityValue, messageObject -> {
                            ChatMessage personalMessage = ChatMessage.fromSerializedString((String) messageObject.body());
                            if (logger.isTraceEnabled()) {
                                logger.trace(String.format("Sending direct message to %s:\n%s", socketId, personalMessage.message));
                            }
                            serverWebSocket.writeTextMessage(personalMessage.message);
                        });
                        if (logger.isTraceEnabled()) {
                            logger.trace(String.format("Identified socket %s as username '%s' and subscribed to event bus channel for direct messages", socketId, identity.get()));
                        }
                        serverWebSocket.writeTextMessage("IdentifiedAs:" + identity.get());

                    } else if (message.startsWith("@") && message.length() > 1) {
                        int indexOfSpace = message.indexOf(" ");
                        if (indexOfSpace != -1) {
                            String destinationIdentity = message.substring(1, indexOfSpace);
                            String messageValue = message.substring(indexOfSpace + 1);
                            if (!destinationIdentity.equalsIgnoreCase(identity.get()) && messageValue.length() > 0) {
                                String identifiedMessage = getIdentifiedMessage(identity, messageValue);
                                ChatMessage addedMessage = redisMessageStream.add(ChatMessage.directMessage(serverWebSocket.binaryHandlerID(), destinationIdentity, identifiedMessage));
                                if (logger.isTraceEnabled()) {
                                    logger.trace(String.format("Sent to redis:\n%s", addedMessage));
                                }
                            }
                        } else {
                            if (logger.isTraceEnabled()) {
                                logger.trace(String.format("Invalid direct message ignored (missing space char):\n%s", message));
                            }
                        }
                    } else {
                        String identifiedMessage = getIdentifiedMessage(identity, message);
                        ChatMessage addedMessage = redisMessageStream.add(ChatMessage.broadcastMessage(serverWebSocket.binaryHandlerID(), identifiedMessage));
                        if (logger.isTraceEnabled()) {
                            logger.trace(String.format("Sent to redis:\n%s", addedMessage));
                        }
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

        private String getIdentifiedMessage(AtomicReference<String> identity, String messageValue) {
            String identifiedMessage;
            if (identity.get().equals("")) {
                identifiedMessage = messageValue;
            } else {
                identifiedMessage = String.format("%s: %s", identity.get(), messageValue);
            }
            return identifiedMessage;
        }

        @Override
        public void close() {
            httpServer.close().result();
        }

        public void broadcastMessage(ChatMessage message) {
            vertx.eventBus().publish(BROADCAST_CHANNEL, message.toSerializedString());
        }

        public void directMessage(ChatMessage message) {
            vertx.eventBus().publish(message.channel, message.toSerializedString());
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
                ChatMessage head = add(ChatMessage.initializer());
                this.offset = head.id;
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
            return message.withId(id);
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
            List<ChatMessage> messagesList = sync.xread(XReadArgs.Builder.block(timeout), streamOffset).stream().map(ChatMessage::fromStreamMessage).collect(Collectors.toList());
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
        Initialize, Direct;

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

    private static class ChatMessage {
        private final ChatMessageType messageType;
        /**
         * The Redis Streams identifier of the message. Composed of a timestamp and an incrementing integer e.g. 123891293129381-1.
         */
        private final String id;
        /**
         * The Vert.x web socket identifier the message was sent from.
         */
        private final String socketId;
        /**
         * The channel to send the message to (If the messageType is Broadcast, this will be hard-wired to BROADCAST_MESSAGE - a reserved channel name).
         */
        private final String channel;
        /**
         * The value of the message.
         */
        private final String message;


        public ChatMessage(ChatMessageType messageType, String id, String socketId, String channel, String message) {
            this.messageType = messageType;
            this.id = id;
            this.socketId = socketId;
            this.channel = channel;
            this.message = message;
        }

        public static ChatMessage initializer() {
            return new ChatMessage(ChatMessageType.Initialize, "", "", "", "");
        }

        public static ChatMessage broadcastMessage(String socketId, String message) {
            return new ChatMessage(ChatMessageType.Broadcast, "", socketId, WebSocketServer.BROADCAST_CHANNEL, message);
        }

        public static ChatMessage directMessage(String socketId, String channel, String message) {
            return new ChatMessage(ChatMessageType.Direct, "", socketId, channel, message);
        }

        public String toSerializedString() {
            return String.format("%s|||%s|||%s|||%s|||%s", messageType.name(), id, socketId, channel, message);
        }

        public static ChatMessage fromSerializedString(String chatMessageString) {
            String[] parts = chatMessageString.split("\\|\\|\\|");
            return new ChatMessage(ChatMessageType.valueOf(parts[0]), parts[1], parts[2], parts[3], parts[4]);
        }

        public Map<String, String> toMap() {
            Map<String, String> messageMap = new HashMap<>();
            messageMap.put("messageType", messageType.name());
            messageMap.put("socketId", socketId);
            messageMap.put("channel", channel);
            messageMap.put("message", message);
            return messageMap;
        }

        public static ChatMessage fromStreamMessage(StreamMessage<String, String> message) {
            if (message == null) {
                return null;
            }
            return new ChatMessage(ChatMessageType.valueOfSafe(message.getBody().get("messageType")), message.getId(), message.getBody().get("socketId"), message.getBody().get("channel"), message.getBody().get("message"));
        }

        public ChatMessage withId(String id) {
            return new ChatMessage(messageType, id, socketId, channel, message);
        }

        @Override
        public String toString() {
            return "ChatMessage{" +
                    "messageType=" + messageType +
                    ", id='" + id + '\'' +
                    ", socketId='" + socketId + '\'' +
                    ", channel='" + channel + '\'' +
                    ", message='" + message + '\'' +
                    '}';
        }
    }
}
