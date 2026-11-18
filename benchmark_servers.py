#!/usr/bin/env python3
"""Build and benchmark the baseline and optimized pub-sub servers."""

from __future__ import annotations

import argparse
import math
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Iterable

PORT = 5555
ROOT = Path(__file__).resolve().parent


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def compile_server(source: Path, output: Path) -> None:
    compiler = os.environ.get("CXX", "g++")
    if not shutil.which(compiler):
        raise RuntimeError("g++ is required but was not found in PATH")
    command = [compiler, "-O3", "-std=c++17", "-pthread", "-march=native", str(source), "-o", str(output)]
    subprocess.run(command, cwd=ROOT, check=True)


def wait_for_server(timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.1)
        try:
            if sock.connect_ex(("127.0.0.1", PORT)) == 0:
                sock.close()
                time.sleep(0.2)
                return
        finally:
            sock.close()
        time.sleep(0.05)
    raise RuntimeError("server did not start listening on port 5555")


def read_line(sock: socket.socket, buffer: bytearray, timeout: float) -> str:
    sock.settimeout(timeout)
    while True:
        newline = buffer.find(b"\n")
        if newline >= 0:
            line = bytes(buffer[:newline]).decode("utf-8", "replace").rstrip("\r")
            del buffer[: newline + 1]
            return line
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("server closed the connection")
        buffer.extend(chunk)


def connect_client(topic: str, subscribe: bool) -> tuple[socket.socket, bytearray]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.connect(("127.0.0.1", PORT))
    buffer = bytearray()
    read_line(sock, buffer, 2.0)  # initial OK
    if subscribe:
        sock.sendall(f"SUB benchmark {topic}\n".encode("ascii"))
        ack = read_line(sock, buffer, 2.0)
        if not ack.startswith("Subscribed to"):
            raise RuntimeError(f"unexpected subscription response: {ack}")
    return sock, buffer


def run_scenario(messages: int, producers: int, consumers: int, warmup: int) -> tuple[float, list[float]]:
    if producers < 1 or consumers < 1:
        raise ValueError("producers and consumers must be at least 1")

    topic = "benchmark_topic"
    producer_sockets = []
    consumer_sockets = []
    consumer_buffers = []

    try:
        for _ in range(producers):
            sock, _ = connect_client(topic, False)
            producer_sockets.append(sock)
        for _ in range(consumers):
            sock, buffer = connect_client(topic, True)
            consumer_sockets.append(sock)
            consumer_buffers.append(buffer)

        # Warm up both the network path and worker-topic routing.
        for i in range(warmup):
            payload = f"warmup-{i}"
            producer_sockets[i % producers].sendall(f"PUB {topic} {payload}\n".encode("ascii"))
            for sock, buffer in zip(consumer_sockets, consumer_buffers):
                read_line(sock, buffer, 5.0)

        latencies_us: list[float] = []
        start = time.perf_counter_ns()

        for i in range(messages):
            message_start = time.perf_counter_ns()
            payload = f"message-{i}"
            producer_sockets[i % producers].sendall(f"PUB {topic} {payload}\n".encode("ascii"))
            for sock, buffer in zip(consumer_sockets, consumer_buffers):
                line = read_line(sock, buffer, 5.0)
                if payload not in line:
                    raise RuntimeError(f"unexpected publication response: {line}")
            message_end = time.perf_counter_ns()
            latencies_us.append((message_end - message_start) / 1000.0)

        elapsed = (time.perf_counter_ns() - start) / 1_000_000_000.0
        return elapsed, latencies_us
    finally:
        for sock in producer_sockets + consumer_sockets:
            sock.close()


def run_server(binary: Path, messages: int, producers: int, consumers: int, warmup: int) -> tuple[float, list[float]]:
    process = subprocess.Popen([str(binary)], cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        wait_for_server()
        return run_scenario(messages, producers, consumers, warmup)
    finally:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)


def print_results(label: str, elapsed: float, latencies: Iterable[float], messages: int) -> None:
    latencies = list(latencies)
    if label:
        print(label)
    print(f"Elapsed seconds: {elapsed:.6f}")
    print(f"Throughput (msgs/s): {messages / elapsed:.2f}")
    print(f"Median latency (us): {percentile(latencies, 0.50):.0f}")
    print(f"99.9th percentile latency (us): {percentile(latencies, 0.999):.0f}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark baseline and optimized pub-sub servers")
    parser.add_argument("--messages", "-n", type=int, default=1000)
    parser.add_argument("--producers", "-p", type=int, default=1)
    parser.add_argument("--consumers", "-c", type=int, default=1)
    parser.add_argument("--warmup", type=int, default=100)
    args = parser.parse_args()

    if args.messages < 1:
        parser.error("--messages must be at least 1")

    if not sys.platform.startswith("linux"):
        print("This benchmark requires Linux epoll support and g++.", file=sys.stderr)
        print("Run it inside a working WSL/Linux shell:", file=sys.stderr)
        print("  cd /mnt/d/Sahishnu/pub_sub", file=sys.stderr)
        print("  python3 benchmark_servers.py --messages 1000 --producers 1 --consumers 1", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="pub_sub_bench_") as temp_dir:
        temp = Path(temp_dir)
        baseline = temp / "server_baseline"
        optimized = temp / "server"
        compile_server(ROOT / "server_baseline.cpp", baseline)
        compile_server(ROOT / "server.cpp", optimized)

        print("Measured Quick Run (sanity)")
        producer_word = "producer" if args.producers == 1 else "producers"
        consumer_word = "consumer" if args.consumers == 1 else "consumers"
        print(f"Configuration: {args.messages} messages, {args.producers} {producer_word}, {args.consumers} {consumer_word}")
        print()

        baseline_elapsed, baseline_latencies = run_server(baseline, args.messages, args.producers, args.consumers, args.warmup)
        print("BASELINE")
        print_results("", baseline_elapsed, baseline_latencies, args.messages)

        optimized_elapsed, optimized_latencies = run_server(optimized, args.messages, args.producers, args.consumers, args.warmup)
        print()
        print("OPTIMIZED")
        print_results("", optimized_elapsed, optimized_latencies, args.messages)

        # A compact relative summary is useful when diagnosing a change.
        if baseline_elapsed > 0:
            print()
            print(f"Throughput speedup: {baseline_elapsed / optimized_elapsed:.2f}x")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
