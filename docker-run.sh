# Run the built container, connecting it to the local redis
CURRENT_IP="$(ifconfig | grep -A 1 'en0' | tail -1 | cut -d ':' -f 2 | cut -d ' ' -f 2)"
docker run --rm -p 8080:8080 --env "REDIS_CONNECTION_STRING=redis://${CURRENT_IP}:18090/0" redis-vertx-chat
