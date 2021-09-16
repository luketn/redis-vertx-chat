package com.mycodefu;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.netty.util.internal.StringUtil;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.ErrorHandler;
import io.vertx.ext.web.handler.StaticHandler;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class RedisChat {
    private static final Logger logger = LogManager.getLogger(RedisChat.class);

    private static final Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(100));
    public static final ObjectMapper jsonObjectMapper = new ObjectMapper();

    public static void main(String[] args) {
        RedisMessageStream redisMessageSenderStream = new RedisMessageStream("Message Sender");
        WebSocketServer webSocketServer = new WebSocketServer(vertx, redisMessageSenderStream);

        Thread readerThread = new Thread(() -> {
            try (RedisMessageStream redisMessageReaderStream = new RedisMessageStream("Message Reader")) {
                while (true) {
                    List<RedisChatMessage> messages = redisMessageReaderStream.read();
                    if (messages.size() > 0) {
                        if (logger.isTraceEnabled()) {
                            logger.trace(String.format("Received %d messages from Redis:\n%s", messages.size(), messages));
                        }
                        for (RedisChatMessage message : messages) {
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
        private final AtomicLong totalWebsocketConnectionsEver = new AtomicLong();
        private final AtomicLong totalWebsocketMessagesReceivedEver = new AtomicLong();
        private final AtomicLong totalWebsocketMessagesSentEver = new AtomicLong();

        private static final String BROADCAST_CHANNEL = "BROADCAST_MESSAGE";

        public WebSocketServer(Vertx vertx, RedisMessageStream redisMessageStream) {
            Router router = Router.router(vertx);
            router.route().failureHandler(ErrorHandler.create(vertx));
            router.route().handler(StaticHandler.create().setCachingEnabled(false));

            httpServer = vertx.createHttpServer();

            router.get("/stats.json").respond(ctx -> Future.succeededFuture(new Stats(totalWebsocketConnectionsEver.get(), totalWebsocketMessagesReceivedEver.get(), totalWebsocketMessagesSentEver.get())));

            httpServer.webSocketHandler(serverWebSocket -> {
                final String socketId = serverWebSocket.binaryHandlerID();
                final AtomicReference<String> identity = new AtomicReference<>("");
                totalWebsocketConnectionsEver.getAndIncrement();

                logger.trace(String.format("New connection received: %s", socketId));

                MessageConsumer<Object> messageConsumer = vertx.eventBus().consumer(BROADCAST_CHANNEL, messageObject -> {
                    RedisChatMessage message = RedisChatMessage.fromSerializedString((String) messageObject.body());
                    switch (message.messageType) {
                        case Broadcast: {
                            if (!message.socketId.equals(socketId)) {
                                if (logger.isTraceEnabled()) {
                                    logger.trace(String.format("Broadcasting message to %s:\n%s", socketId, message.message));
                                }
                                try {
                                    writeMessage(serverWebSocket, jsonObjectMapper.writeValueAsString(BrowserChatMessage.fromRedisStreamMessage(message)));
                                } catch (JsonProcessingException e) {
                                    logger.error("Failed to serialize JSON message to client.", e);
                                }

                            } else {
                                if (logger.isTraceEnabled()) {
                                    logger.trace(String.format("Ignoring message broadcast from self:\n%s", message.message));
                                }
                            }
                            break;
                        }
                    }
                });

                serverWebSocket.handler(messageRawJsonStringBuffer -> {
                    try {
                        String messageRawJsonString = messageRawJsonStringBuffer.toString();
                        BrowserChatMessage browserChatMessage = jsonObjectMapper.readValue(messageRawJsonString, BrowserChatMessage.class);

                        totalWebsocketMessagesReceivedEver.getAndIncrement();
                        if (logger.isTraceEnabled()) {
                            logger.trace(String.format("Message received from web socket %s:\n%s", socketId, browserChatMessage));
                        }
                        switch (browserChatMessage.messageType) {
                            case Identify: {
                                identity.set(browserChatMessage.value);
                                vertx.eventBus().consumer(identity.get(), messageObject -> {
                                    RedisChatMessage personalMessage = RedisChatMessage.fromSerializedString((String) messageObject.body());
                                    if (logger.isTraceEnabled()) {
                                        logger.trace(String.format("Sending direct message to %s (%s):\n%s", identity.get(), socketId, personalMessage.message));
                                    }
                                    try {
                                        writeMessage(serverWebSocket, jsonObjectMapper.writeValueAsString(BrowserChatMessage.fromRedisStreamMessage(personalMessage)));
                                    } catch (JsonProcessingException e) {
                                        logger.error("Failed to serialize JSON message to client.", e);
                                    }

                                });
                                if (logger.isTraceEnabled()) {
                                    logger.trace(String.format("Identified socket %s as username '%s' and subscribed to event bus channel for direct messages", socketId, identity.get()));
                                }
                                writeMessage(serverWebSocket, BrowserChatMessage.identifiedAs(identity.get()));
                                break;
                            }
                            case Direct: {
                                if (!browserChatMessage.to.equalsIgnoreCase(identity.get()) && browserChatMessage.value != null && browserChatMessage.value.length() > 0) {
                                    RedisChatMessage addedMessage = redisMessageStream.add(RedisChatMessage.directMessage(serverWebSocket.binaryHandlerID(), identity.get(), browserChatMessage.to, browserChatMessage.value));
                                    if (logger.isTraceEnabled()) {
                                        logger.trace(String.format("Sent to redis:\n%s", addedMessage));
                                    }
                                } else {
                                    if (logger.isTraceEnabled()) {
                                        logger.trace(String.format("Ignoring direct message on socket identity '%s':\n%s", identity.get(), messageRawJsonString));
                                    }
                                }
                                break;
                            }
                            case Broadcast: {
                                RedisChatMessage addedMessage = redisMessageStream.add(RedisChatMessage.broadcastMessage(serverWebSocket.binaryHandlerID(), identity.get(), browserChatMessage.value));
                                if (logger.isTraceEnabled()) {
                                    logger.trace(String.format("Sent to redis:\n%s", addedMessage));
                                }
                                break;
                            }
                        }
                    } catch (JsonProcessingException e) {
                        logger.error(String.format("Received a malformed message from the browser:\n%s", messageRawJsonStringBuffer), e);
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

        private void writeMessage(ServerWebSocket serverWebSocket, BrowserChatMessage message) {
            try {
                writeMessage(serverWebSocket, jsonObjectMapper.writeValueAsString(message));
            } catch (JsonProcessingException e) {
                logger.error(String.format("Failed to serialize message to JSON:\n%s", message), e);
            }
        }

        private void writeMessage(ServerWebSocket serverWebSocket, String message) {
            totalWebsocketMessagesSentEver.getAndIncrement();
            serverWebSocket.writeTextMessage(message);
        }

        @Override
        public void close() {
            httpServer.close().result();
        }

        public void broadcastMessage(RedisChatMessage message) {
            vertx.eventBus().publish(BROADCAST_CHANNEL, message.toSerializedString());
        }

        public void directMessage(RedisChatMessage message) {
            vertx.eventBus().publish(message.to, message.toSerializedString());
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
                RedisChatMessage head = add(RedisChatMessage.initializer());
                this.offset = head.id;
            } else {
                this.offset = offset;
            }
        }

        /**
         * Append a message to the Redis stream 'messages'.
         */
        public RedisChatMessage add(RedisChatMessage message) {
            //XADD messages * messageType "Broadcast" socketId "__vertx.ws.de4a22c7-e925-4c65-ac22-820a964c7041" message "Hi there"
            Map<String, String> messageMap = message.toRedisMessage();
            String id = connection.sync().xadd("messages", messageMap);
            return message.withId(id);
        }

        public List<RedisChatMessage> read() {
            return read(Duration.ZERO);
        }

        /**
         * Return a set of messages from the Redis stream 'messages' (blocking for duration).
         * Intended to be called in a loop with a short duration.
         */
        public List<RedisChatMessage> read(Duration timeout) {
            //XREAD BLOCK 0 STREAMS messages 1631516018887-0
            XReadArgs.StreamOffset<String> streamOffset = XReadArgs.StreamOffset.from("messages", offset);
            RedisCommands<String, String> sync = connection.sync();
            sync.setTimeout(timeout);
            List<RedisChatMessage> messagesList = sync.xread(XReadArgs.Builder.block(timeout), streamOffset).stream().map(RedisChatMessage::fromRedisMessage).collect(Collectors.toList());
            if (messagesList.size() > 0) {
                this.offset = messagesList.get(messagesList.size() - 1).id;
            }
            return messagesList;
        }

        public RedisChatMessage readHead() {
            //XREVRANGE messages + - COUNT 1
            List<StreamMessage<String, String>> messages = connection.sync().xrevrange("messages", Range.unbounded(), Limit.from(1));
            if (messages != null && messages.size() > 0) {
                return RedisChatMessage.fromRedisMessage(messages.get(0));
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

    private enum RedisChatMessageType {
        None,
        Broadcast,
        Initialize, Direct;

        public static RedisChatMessageType valueOfSafe(String messageTypeName) {
            if (messageTypeName == null || messageTypeName.length() == 0) {
                return RedisChatMessageType.None;
            } else {
                try {
                    return RedisChatMessageType.valueOf(messageTypeName);
                } catch (IllegalArgumentException e) {
                    return RedisChatMessageType.None;
                }
            }
        }
    }

    private enum BrowserChatMessageType {
        Identify,
        IdentifiedAs,
        Broadcast,
        Direct
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private static class BrowserChatMessage {
        private String datastoreId;
        private BrowserChatMessageType messageType;
        private String from;
        private String to;
        private String value;

        public static BrowserChatMessage identifiedAs(String username) {
            BrowserChatMessage browserChatMessage = new BrowserChatMessage();
            browserChatMessage.messageType = BrowserChatMessageType.IdentifiedAs;
            browserChatMessage.value = username;
            return browserChatMessage;
        }

        public static BrowserChatMessage fromRedisStreamMessage(RedisChatMessage redisChatMessage) {
            BrowserChatMessage browserChatMessage = new BrowserChatMessage();
            switch (redisChatMessage.messageType) {
                case Broadcast: {
                    browserChatMessage.messageType = BrowserChatMessageType.Broadcast;
                    browserChatMessage.from = redisChatMessage.from;
                    break;
                }
                case Direct: {
                    browserChatMessage.messageType = BrowserChatMessageType.Direct;
                    browserChatMessage.from = redisChatMessage.from;
                    browserChatMessage.to = redisChatMessage.to;
                    break;
                }
            }
            browserChatMessage.datastoreId = redisChatMessage.id;
            browserChatMessage.value = redisChatMessage.message;
            return browserChatMessage;
        }

        public String getFrom() {
            return from;
        }

        public String getDatastoreId() {
            return datastoreId;
        }

        public BrowserChatMessageType getMessageType() {
            return messageType;
        }

        public String getTo() {
            return to;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            try {
                return "BrowserChatMessage" + jsonObjectMapper.writeValueAsString(this);
            } catch (JsonProcessingException e) {return "BAD";}
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private static class RedisChatMessage {
        private final RedisChatMessageType messageType;
        /**
         * The Redis Streams identifier of the message. Composed of a timestamp and an incrementing integer e.g. 123891293129381-1.
         */
        private final String id;
        /**
         * The Vert.x web socket identifier the message was sent from.
         */
        private final String socketId;
        /**
         * The person to send the message to (If the messageType is Broadcast, this will be hard-wired to "").
         */
        private final String to;
        /**
         * The person the message is from.
         */
        private final String from;
        /**
         * The value of the message.
         */
        private final String message;

        public RedisChatMessage() {
            this(RedisChatMessageType.None, "", "", "", "", "");
        }

        public RedisChatMessage(RedisChatMessageType messageType, String id, String socketId, String from, String to, String message) {
            this.messageType = messageType;
            this.id = id;
            this.socketId = socketId;
            this.from = from;
            this.to = to;
            this.message = message;
        }

        public static RedisChatMessage initializer() {
            return new RedisChatMessage(RedisChatMessageType.Initialize, "", "", "", "", "");
        }

        public static RedisChatMessage broadcastMessage(String socketId, String from, String message) {
            return new RedisChatMessage(RedisChatMessageType.Broadcast, "", socketId, from, "", message);
        }

        public static RedisChatMessage directMessage(String socketId, String from, String to, String message) {
            return new RedisChatMessage(RedisChatMessageType.Direct, "", socketId, from, to, message);
        }

        public String toSerializedString() {
            try {
                return jsonObjectMapper.writeValueAsString(this);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Serialization error.", e);
            }
        }

        public static RedisChatMessage fromSerializedString(String chatMessageString) {
            try {
                return jsonObjectMapper.readValue(chatMessageString, RedisChatMessage.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Serialization error.", e);
            }
        }

        public Map<String, String> toRedisMessage() {
            Map<String, String> messageMap = new HashMap<>();
            messageMap.put("messageType", messageType.name());
            if (!StringUtil.isNullOrEmpty(socketId)) {
                messageMap.put("socketId", socketId);
            }
            if (!StringUtil.isNullOrEmpty(from)) {
                messageMap.put("from", from);
            }
            if (!StringUtil.isNullOrEmpty(to)) {
                messageMap.put("to", to);
            }
            if (!StringUtil.isNullOrEmpty(message)) {
                messageMap.put("message", message);
            }
            return messageMap;
        }

        public static RedisChatMessage fromRedisMessage(StreamMessage<String, String> message) {
            if (message == null) {
                return null;
            }
            return new RedisChatMessage(RedisChatMessageType.valueOfSafe(valueOrEmptyString(message, "messageType")), message.getId(), valueOrEmptyString(message, "socketId"), valueOrEmptyString(message, "from"), valueOrEmptyString(message, "to"), valueOrEmptyString(message, "message"));
        }

        private static String valueOrEmptyString(StreamMessage<String, String> message, String fieldName) {
            String value = message.getBody().get(fieldName);
            return value == null ? "" : value;
        }

        public RedisChatMessage withId(String id) {
            return new RedisChatMessage(messageType, id, socketId, from, to, message);
        }

        @Override
        public String toString() {
            try {
                return "ChatMessage" + jsonObjectMapper.writeValueAsString(this);
            } catch (JsonProcessingException e) {return "BAD";}
        }

        public RedisChatMessageType getMessageType() {
            return messageType;
        }

        public String getId() {
            return id;
        }

        public String getSocketId() {
            return socketId;
        }

        public String getTo() {
            return to;
        }

        public String getFrom() {
            return from;
        }

        public String getMessage() {
            return message;
        }
    }

    private static class Stats {
        private long totalSocketConnectionsEver;
        private long totalSocketMessagesReceivedEver;
        private long totalSocketMessagesSentEver;

        public Stats(long totalSocketConnectionsEver, long totalSocketMessagesReceivedEver, long totalSocketMessagesSentEver) {
            this.totalSocketConnectionsEver = totalSocketConnectionsEver;
            this.totalSocketMessagesReceivedEver = totalSocketMessagesReceivedEver;
            this.totalSocketMessagesSentEver = totalSocketMessagesSentEver;
        }

        public long getTotalSocketConnectionsEver() {
            return totalSocketConnectionsEver;
        }

        public long getTotalSocketMessagesReceivedEver() {
            return totalSocketMessagesReceivedEver;
        }

        public long getTotalSocketMessagesSentEver() {
            return totalSocketMessagesSentEver;
        }
    }
}
