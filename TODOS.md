A:
- Change format to 1 msg per record include payload
- Fix the hash logic - it aint quite right need to *s it again
- Message storage adapter
- Metrics
- Container

B:
- Prototype the failover & scale-up/scale-down logic
- Need pipeline redis client

## Load Test
- For load test export: redis memory, throughput, jitter, redis call timing, millis behind
- Worker calls per sec - gets,writes,checkpoints
- Consumer - calls per sec, jitter from stream
- 200 requests per sec (~10x) 
- variables: buckets, length of time (prototype 1 - 5 - 15 - 30 mins)
- load generator - artillery
- min size instance for reasonable consumption latency
