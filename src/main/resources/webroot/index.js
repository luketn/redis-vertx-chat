new function () {
    var ws = null;
    var connected = false;
    var connecting = false;
    var lastConnectFailed = false;
    var serverUrl;

    var connectionStatus;
    var sendMessage;
    var username;

    var connectButton;
    var disconnectButton;
    var sendButton;
    var identifyButton;

    var open = function () {
        var url = serverUrl.val();
        try {
            ws = new WebSocket(url);
        } catch (e) {
            connectionStatus.text('FAILED TO CONNECT: ' + e);
            return;
        }
        ws.onopen = onOpen;
        ws.onclose = onClose;
        ws.onmessage = onMessage;
        ws.onerror = onError;

        connecting = true;
        connectionStatus.text('OPENING ...');
        serverUrl.attr('disabled', 'disabled');
        connectButton.attr('disabled', 'disabled');
        disconnectButton.attr('disabled', 'disabled');
    }

    var close = function (event) {
        if (ws) {
            console.log('CLOSING ...');
            ws.close();
        }
        connecting = false;
        connected = false;
        if (lastConnectFailed) {
            if (event) {
                if (event.code == 1000)
                    reason = "Normal closure, meaning that the purpose for which the connection was established has been fulfilled.";
                else if (event.code == 1001)
                    reason = "An endpoint is \"going away\", such as a server going down or a browser having navigated away from a page.";
                else if (event.code == 1002)
                    reason = "An endpoint is terminating the connection due to a protocol error";
                else if (event.code == 1003)
                    reason = "An endpoint is terminating the connection because it has received a type of data it cannot accept (e.g., an endpoint that understands only text data MAY send this if it receives a binary message).";
                else if (event.code == 1004)
                    reason = "Reserved. The specific meaning might be defined in the future.";
                else if (event.code == 1005)
                    reason = "No status code was actually present.";
                else if (event.code == 1006)
                    reason = "The connection was closed abnormally, e.g., without sending or receiving a Close control frame";
                else if (event.code == 1007)
                    reason = "An endpoint is terminating the connection because it has received data within a message that was not consistent with the type of the message (e.g., non-UTF-8 [http://tools.ietf.org/html/rfc3629] data within a text message).";
                else if (event.code == 1008)
                    reason = "An endpoint is terminating the connection because it has received a message that \"violates its policy\". This reason is given either if there is no other sutible reason, or if there is a need to hide specific details about the policy.";
                else if (event.code == 1009)
                    reason = "An endpoint is terminating the connection because it has received a message that is too big for it to process.";
                else if (event.code == 1010) // Note that this status code is not used by the server, because it can fail the WebSocket handshake instead.
                    reason = "An endpoint (client) is terminating the connection because it has expected the server to negotiate one or more extension, but the server didn't return them in the response message of the WebSocket handshake. <br /> Specifically, the extensions that are needed are: " + event.reason;
                else if (event.code == 1011)
                    reason = "A server is terminating the connection because it encountered an unexpected condition that prevented it from fulfilling the request.";
                else if (event.code == 1015)
                    reason = "The connection was closed due to a failure to perform a TLS handshake (e.g., the server certificate can't be verified).";
                else
                    reason = "Unknown reason";

                connectionStatus.text(connectionStatus.text() + '\n' + reason);
            }
        } else {
            connectionStatus.text('CLOSED');
        }
        serverUrl.removeAttr('disabled');
        disconnectButton.attr('disabled', 'disabled');
        connectButton.removeAttr('disabled');
        sendMessage.attr('disabled', 'disabled');
        sendButton.attr('disabled', 'disabled');
        username.removeAttr('disabled');
        identifyButton.removeAttr('disabled');
    }

    var clearLog = function () {
        $('#messages').html('');
    }

    var identifyUser = function () {
        var usernameValue = username.val();
        addMessage('Identified as "' + usernameValue + '"', 'SENT');
        ws.send(JSON.stringify({
            messageType: "Identify",
            value: usernameValue
        }));
        username.attr('disabled', 'disabled');
        identifyButton.attr('disabled', 'disabled');
    }

    var onOpen = function () {
        console.log('OPENED: ' + serverUrl.val());
        connecting = false;
        connected = true;
        lastConnectFailed = false;
        connectionStatus.text('OPENED');
        sendMessage.removeAttr('disabled');
        sendButton.removeAttr('disabled');
        username.removeAttr('disabled');
        identifyButton.removeAttr('disabled');
        disconnectButton.removeAttr('disabled');
    };

    var onClose = function (event) {
        console.log('CLOSED: ' + serverUrl.val(), event);
        ws = null;
        close(event);
    };

    function addMessageFromBroadcastOrDirect(data) {
        localStorage.setItem("lastReadDatastoreId", data.datastoreId);
        addMessage(data.from + ": " + data.value);
    }

    var onMessage = function (event) {
        var data = JSON.parse(event.data);
        switch (data.messageType) {
            case "IdentifiedAs": {
                localStorage.setItem("username", data.value);

                addMessage("Identified as " + data.value);

                let lastReadDatastoreId = localStorage.getItem("lastReadDatastoreId");
                if (lastReadDatastoreId) {
                    $.get(window.location.href + "getMessagesSince.json?to=" + data.value + "&lastReadDatastoreId=" + lastReadDatastoreId, (messages) => {
                        for (const message of messages) {
                            addMessageFromBroadcastOrDirect(message);
                        }

                    });
                }
                break;
            }
            case "Broadcast":
            case "Direct": {
                addMessageFromBroadcastOrDirect(data);
                break;
            }
            default: {
                addMessage(event.data);
            }
        }
    };

    var onError = function (event) {
        if (connecting) {
            connecting = false;
            lastConnectFailed = true;
            connectionStatus.text('FAILED TO CONNECT!');
            console.log("FAILED TO CONNECT", event);
        } else {
            connectionStatus.text('ERROR OCCURRED: ' + event.data);
        }
    }

    var addMessage = function (data, type) {
        var msg = $('<pre>').text(data);
        if (type === 'SENT') {
            msg.addClass('sent');
        }
        var messages = $('#messages');
        messages.append(msg);

        var msgBox = messages.get(0);
        while (msgBox.childNodes.length > 100) {
            msgBox.removeChild(msgBox.firstChild);
        }
        msgBox.scrollTop = msgBox.scrollHeight;
    }

    WebSocketClient = {
        init: function () {
            serverUrl = $('#serverUrl');
            connectionStatus = $('#connectionStatus');
            sendMessage = $('#sendMessage');
            username = $('#username');
            if (localStorage.getItem('username')){
                username.val(localStorage.getItem('username'));
            }

            connectButton = $('#connectButton');
            disconnectButton = $('#disconnectButton');
            sendButton = $('#sendButton');
            identifyButton = $('#identifyButton');

            connectButton.click(function (e) {
                close();
                open();
            });

            disconnectButton.click(function (e) {
                close();
            });

            sendButton.click(function (e) {
                var msg = $('#sendMessage').val();
                addMessage(msg, 'SENT');
                if (msg.substr(0, 1) === "@") {
                    var to = msg.substr(1);
                    let indexOfSpace = msg.indexOf(" ");
                    if (indexOfSpace !== -1) {
                        to = msg.substr(1, indexOfSpace - 1);
                        msg = msg.substr(indexOfSpace + 1);
                    }
                    ws.send(JSON.stringify({
                        messageType: "Direct",
                        to: to,
                        value: msg
                    }));
                } else {
                    ws.send(JSON.stringify({
                        messageType: "Broadcast",
                        value: msg
                    }));
                }
            });

            $('#clearMessage').click(function (e) {
                clearLog();
            });

            $('#identifyButton').click(function (e) {
                identifyUser();
            });

            var isCtrl;
            sendMessage.keyup(function (e) {
                if (e.which == 17) isCtrl = false;
            }).keydown(function (e) {
                if (e.which == 17) isCtrl = true;
                if (e.which == 13 && isCtrl == true) {
                    sendButton.click();
                    return false;
                }
            });

            let protocol;
            let wsUrl;
            if (window.location.protocol === 'https') {
                protocol = 'wss';
                wsUrl = protocol + window.location.href.substr(5);
            } else {
                protocol = 'ws';
                wsUrl = protocol + window.location.href.substr(4);
            }
            $('#serverUrl').val(wsUrl);

            setInterval(() => {
                let legend = $('#legend');
                $.get(window.location.href + "stats.json", (data) => {
                    legend.text('Server (total sockets ever: ' + data.totalSocketConnectionsEver + ', total messages received ever: ' + data.totalSocketMessagesReceivedEver + ', total messages sent ever: ' + data.totalSocketMessagesSentEver + ')');
                });
            }, 1000);
        }
    };
}

$(function () {
    WebSocketClient.init();
});