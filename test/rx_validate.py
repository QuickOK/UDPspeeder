#!/usr/bin/env python3
"""Loopback validation for [report_fec_rx]: server+client through a lossy UDP
proxy on 127.0.0.1. No loss -> no recovery; moderate loss -> recovery with a
near-zero delivery gap; heavy loss -> failed groups. Exit 0 iff all pass."""
import random
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

BIN = "./speederv2"
KEY = "rxtest"
N_PKTS = 4000
PAYLOAD = b"x" * 200
ANSI = re.compile(r"\x1b\[[0-9;]*m")
RX_RE = re.compile(r"\[report_fec_rx\]pkt_ok:(\d+) pkt_rec:(\d+) grp_ok:(\d+) "
                   r"grp_rec:(\d+) grp_fail:(\d+) shard_lost:(\d+) par_waste:(\d+)")


def free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class LossyProxy(threading.Thread):
    """UDP proxy dropping a seeded fraction of client->server datagrams.
    Replies (server->client) pass through untouched."""

    def __init__(self, listen_port, dst_port, drop):
        super().__init__(daemon=True)
        self.drop = drop
        self.dst = ("127.0.0.1", dst_port)
        self.rng = random.Random(1234)
        self.stop_flag = False
        self.client_addr = None
        self.front = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.front.bind(("127.0.0.1", listen_port))
        self.front.settimeout(0.1)
        self.back = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.back.settimeout(0.1)
        self.back.bind(("127.0.0.1", 0))

    def run(self):
        def pump_back():
            while not self.stop_flag:
                try:
                    data, _ = self.back.recvfrom(65535)
                except socket.timeout:
                    continue
                except OSError:
                    return
                if self.client_addr:
                    self.front.sendto(data, self.client_addr)
        threading.Thread(target=pump_back, daemon=True).start()
        while not self.stop_flag:
            try:
                data, addr = self.front.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                return
            self.client_addr = addr
            if self.rng.random() >= self.drop:
                self.back.sendto(data, self.dst)


class Sink(threading.Thread):
    """Counts datagrams arriving at the tunnel's far end."""

    def __init__(self, port):
        super().__init__(daemon=True)
        self.count = 0
        self.stop_flag = False
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", port))
        self.sock.settimeout(0.1)

    def run(self):
        while not self.stop_flag:
            try:
                self.sock.recvfrom(65535)
                self.count += 1
            except socket.timeout:
                continue
            except OSError:
                return


def spawn(args, logfile):
    return subprocess.Popen(args, stdout=logfile, stderr=subprocess.STDOUT)


def last_rx_counters(path):
    text = ANSI.sub("", open(path, encoding="utf-8", errors="replace").read())
    hits = RX_RE.findall(text)
    if not hits:
        return None
    keys = ("pkt_ok", "pkt_rec", "grp_ok", "grp_rec", "grp_fail",
            "shard_lost", "par_waste")
    return dict(zip(keys, (int(v) for v in hits[-1])))


def run_case(drop):
    sink_port, server_port, proxy_port, client_port = (
        free_port(), free_port(), free_port(), free_port())
    sink = Sink(sink_port); sink.start()
    proxy = LossyProxy(proxy_port, server_port, drop); proxy.start()
    slog = tempfile.NamedTemporaryFile("w", suffix=".server.log", delete=False)
    clog = tempfile.NamedTemporaryFile("w", suffix=".client.log", delete=False)
    server = spawn([BIN, "-s", "-l", f"127.0.0.1:{server_port}",
                    "-r", f"127.0.0.1:{sink_port}", "-f8:4", "--mode", "0",
                    "--report", "1", "-k", KEY], slog)
    time.sleep(0.3)
    client = spawn([BIN, "-c", "-l", f"127.0.0.1:{client_port}",
                    "-r", f"127.0.0.1:{proxy_port}", "-f8:4", "--mode", "0",
                    "--report", "1", "-k", KEY], clog)
    time.sleep(0.5)
    tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for _ in range(N_PKTS):
        tx.sendto(PAYLOAD, ("127.0.0.1", client_port))
        time.sleep(0.002)
    time.sleep(3.0)  # drain + at least one more report tick
    for p in (client, server):
        p.send_signal(signal.SIGTERM)
        p.wait(timeout=5)
    proxy.stop_flag = True
    sink.stop_flag = True
    c = last_rx_counters(slog.name)
    assert c is not None, f"no [report_fec_rx] line in {slog.name}"
    return c, sink.count


def main():
    failures = []

    c, delivered = run_case(0.0)
    print(f"drop=0%   sink={delivered}/{N_PKTS} counters={c}")
    if c["pkt_rec"] != 0: failures.append("0%: expected pkt_rec==0")
    if c["grp_fail"] != 0: failures.append("0%: expected grp_fail==0")
    if c["pkt_ok"] < N_PKTS * 0.99: failures.append("0%: pkt_ok below sent count")

    c, delivered = run_case(0.15)
    print(f"drop=15%  sink={delivered}/{N_PKTS} counters={c}")
    if c["pkt_rec"] == 0: failures.append("15%: expected pkt_rec>0")
    if delivered < N_PKTS * 0.95: failures.append("15%: FEC should hold delivery >=95%")

    c, delivered = run_case(0.45)
    print(f"drop=45%  sink={delivered}/{N_PKTS} counters={c}")
    if c["grp_fail"] == 0: failures.append("45%: expected grp_fail>0")
    if c["shard_lost"] == 0: failures.append("45%: expected shard_lost>0")

    if failures:
        print("FAIL:\n  " + "\n  ".join(failures))
        sys.exit(1)
    print("all cases passed")


if __name__ == "__main__":
    main()
