# Run a local instance of Redis for debugging and local development as a Docker daemon (-d) process
mkdir -p "$(pwd)/data"
docker run -d --rm --name VertXRedis -p 18090:6379 -v "$(pwd)/data:/data" redis:6-alpine redis-server --appendonly yes

# After you have the Daemon running, you can use the Redis CLI for ad-hoc queries like this:
# docker exec -it VertXRedis redis-cli
