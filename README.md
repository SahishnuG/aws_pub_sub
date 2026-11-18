# Low-Latency Pub/Sub Server: Run Guide (AWS EC2)

C++17 epoll pub/sub server (4 I/O threads, 10 topic-sharded workers, lock-free queues, custom memory pool) with a Streamlit client.

```
Browser --HTTP :8501--> Streamlit (app.py) --TCP 127.0.0.1:5555--> C++ server (server.cpp)
```

Only port 22 (your IP) and 8501 are open in the security group. **Never open 5555.**

---

## 1. SSH into the instance

Your IP changes (hotspot, Wi-Fi), so update the security group first:

```powershell
curl.exe -4 https://checkip.amazonaws.com
```

AWS Console -> EC2 -> Security Groups -> Edit inbound rules -> SSH, port 22, source `<that ip>/32`.

```powershell
ssh -i D:\Downloads\pubsub-key.pem ubuntu@<instance_public_ipv4>
```

The public IPv4 changes every time the instance is stopped and started (unless you attach an Elastic IP).

### Copy files from Windows to EC2

Run from the project folder on your PC:

```powershell
scp -i D:\Downloads\pubsub-key.pem server.cpp memory_pool.h concurrentqueue.h lightweightsemaphore.h app.py pubsub_client.py ubuntu@<instance_public_ipv4>:~/pubsub/
```

For benchmarking, also copy `server_baseline.cpp` and `final_benchmark.py`.

---

## 2. Stop everything

### C++ server

```bash
cd ~/pubsub
pgrep -a server              # is it running? (e.g. 3167 ./server)
tmux list-sessions
tmux kill-session -t pubsub
sudo fuser 5555/tcp          # prints nothing when the port is free
```

### Streamlit

```bash
pgrep -a streamlit           # is it running?
pkill -f streamlit           # stop it
```

---

## 3. Build and run

### One-time: swap (the free instance has 1 GB RAM)

```bash
free -h
```

If the Swap row shows 0:

```bash
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile && sudo swapon /swapfile
```

### Server (in tmux)

```bash
cd ~/pubsub
g++ -O2 -std=c++17 -pthread server.cpp -o server
tmux new -s pubsub
./server
```

You should see 10 workers and 4 I/O threads come online. Detach with `Ctrl+b`, then `d` (pressed slowly). Reattach with `tmux attach -t pubsub`.

`MemoryPool taskPool(5000)` in `server.cpp` is sized for the 1 GB instance (about 20 MB). Do not use 500000 here (about 2 GB).

### Streamlit client (new SSH window, if not already running)

```bash
cd ~/pubsub && source venv/bin/activate
APP_PASSWORD='choose-a-strong-password' nohup streamlit run app.py \
  --server.address 0.0.0.0 --server.port 8501 --server.headless true > streamlit.log 2>&1 &
```

Open `http://<instance_public_ipv4>:8501`, enter the password, click **Start/Connect to C++ Server**, then subscribe and publish.

Check the logs:

```bash
tail -n 20 streamlit.log
```

---

## 4. Restart the server without killing tmux

```bash
pgrep -a server
# 3123 tmux new -s pubsub
# 3167 ./server
kill 3167
```

```bash
tmux attach -t pubsub
cd ~/pubsub
./server
```

After any server restart, existing browser sessions lose their connection. Click **Start/Connect** again and re-subscribe, or open a fresh tab.

If you changed `server.cpp`, rebuild first: `g++ -O2 -std=c++17 -pthread server.cpp -o server`.

---

## 5. Benchmark

Stop the running server first (section 2), because the benchmark starts its own servers on port 5555.

```bash
cd ~/pubsub
python3 final_benchmark.py --messages 10000 --producers 4 --consumers 4 --warmup 1000 --runs 5
```

It compiles `server.cpp` and `server_baseline.cpp`, alternates baseline and optimized runs, and prints throughput, median and p99.9 latency.

Notes:
- The workers in this build busy-poll (`yield()`), so idle CPU is high (about 190% on 2 vCPUs). Stop the server when you are not using it.
- The test sends one message at a time on a single topic, so it measures delivery latency, not scaling under contention.

---

## 6. Troubleshooting

| Symptom | Fix |
|---|---|
| SSH times out | Your IP changed. Update the SSH rule (section 1). |
| `Permission denied (publickey)` | Wrong key file or username (`ubuntu`). Check the key pair name on the instance. |
| UNPROTECTED PRIVATE KEY FILE | `icacls <key> /inheritance:r` then `icacls <key> /grant:r "$($env:USERNAME):(R)"` |
| Page does not load | Port 8501 is not open in the security group, or Streamlit is not running (`pgrep -a streamlit`). |
| "Connection failed" in the sidebar | The server is not running, or `app.py` is not using `host="127.0.0.1"`. |
| Server killed at startup | Out of memory. Check the pool size and `free -h`. |
| `Killed` while compiling | Swap is not active (section 3). |
| `Address already in use` | Something already holds 5555: `sudo fuser 5555/tcp`, then stop it. |

---

## 7. Cost and security

- Stop the instance from the console when you are not demoing. A stopped instance does not bill for compute.
- Keep a budget alert on in Billing.
- SSH is limited to your IP. 8501 is public, so use a strong `APP_PASSWORD`; the page is plain HTTP.
- The server has no authentication of its own. It is protected by the closed port 5555 and the Streamlit password.
