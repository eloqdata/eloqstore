#!/usr/bin/env bash
# Simulate a weak network for specific ports (e.g., 9000 and 5572)
# Usage:
#   sudo ./test_rclone_timeout.sh start
#   sudo ./test_rclone_timeout.sh stop

IFACE="lo"           # Network interface (loopback if MinIO and rclone run locally)
PORTS=(9000)         # Ports to throttle
DELAY="300ms"        # Average delay
JITTER="100ms"       # Jitter
LOSS="10%"           # Packet loss rate

start() {
    echo "🕸️ Simulating weak network: delay=$DELAY jitter=$JITTER loss=$LOSS"
    echo "⚙️  Target ports: ${PORTS[*]}"
    # Create a priority root qdisc
    tc qdisc add dev $IFACE root handle 1: prio
    # Add a netem qdisc for latency/loss simulation
    tc qdisc add dev $IFACE parent 1:3 handle 30: netem delay $DELAY $JITTER loss $LOSS
    # Add filters for each target port
    for port in "${PORTS[@]}"; do
        echo "🎯 Applying limits to port $port"
        tc filter add dev $IFACE protocol ip parent 1:0 prio 3 u32 \
            match ip dport $port 0xffff flowid 1:3
    done
}

stop() {
    echo "♻️ Restoring normal network conditions"
    tc qdisc del dev $IFACE root 2>/dev/null
}

case "$1" in
    start) start ;;
    stop)  stop ;;
    *) echo "Usage: sudo $0 start | stop" ;;
esac
