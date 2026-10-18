#!/bin/sh
# Isolated three-master fixture; loopback client ports and no host-mounted data.
set -eu
mode=${1:?usage: redis-cluster.sh start|stop anylock-test-NAME}
cluster_name=${2:?provide an isolated anylock-test- container name}
case "$cluster_name" in
    anylock-test-*) ;;
    *) printf 'container name must start with anylock-test-\n' >&2; exit 2 ;;
esac
case "$mode" in
    stop) docker rm -f "$cluster_name"; exit ;;
    start) ;;
    *) printf 'expected start or stop\n' >&2; exit 2 ;;
esac
docker run -d --name "$cluster_name" \
    -p 127.0.0.1:7000:7000 -p 127.0.0.1:7001:7001 -p 127.0.0.1:7002:7002 \
    "${ANYLOCK_REDIS_CLUSTER_IMAGE:-redis:7-alpine}" sh -c '
    for port in 7000 7001 7002; do
        mkdir -p "/data/$port"
        redis-server --port "$port" --dir "/data/$port" \
            --cluster-enabled yes --cluster-config-file nodes.conf \
            --cluster-node-timeout 5000 --cluster-announce-ip 127.0.0.1 \
            --bind 0.0.0.0 --protected-mode no --save "" --appendonly no --daemonize yes
    done
    exec tail -f /dev/null
'
ready() {
    docker exec "$cluster_name" sh -c '
        for port in 7000 7001 7002; do
            redis-cli -p "$port" ping | grep -q PONG || exit 1
        done
    '
}
for attempt in 1 2 3 4 5 6 7 8 9 10; do
    if ready; then break; fi
    if [ "$attempt" -eq 10 ]; then printf 'Redis nodes did not start\n' >&2; exit 1; fi
    sleep 1
done
docker exec "$cluster_name" timeout 30 redis-cli --cluster create \
    127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 --cluster-replicas 0 --cluster-yes
for attempt in 1 2 3 4 5 6 7 8 9 10; do
    if docker exec "$cluster_name" sh -c '
        for port in 7000 7001 7002; do
            redis-cli -p "$port" cluster info | grep -q "cluster_state:ok" || exit 1
        done
    '; then exit 0; fi
    if [ "$attempt" -eq 10 ]; then printf 'Redis Cluster did not become healthy\n' >&2; exit 1; fi
    sleep 1
done
