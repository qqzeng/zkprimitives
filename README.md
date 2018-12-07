# zkprimitives
This repository contains some representative zookeeper primitives implementation. Put to another word, 
they are practical cases in distributed application scenes which ZooKeeper is able to participate in.
Specifically, these more powerful ZooKeeper primitives including Leader Election, Configuration Management, and Lock (Exclusive Lock, Advanced Exclusive Lock and ReadWrite Lock).

## Lock
It's worth being noted that even though ZooKeeper is wait-free, it still can be used to implement efficient blocking primitives. 

### Exclusive Lock
Exclusive lock, which is corresponding to Simple Lock in paper. It suffers from the **Herd Effect**, that is to say, if there are many clients waiting to acquire a lock, they will all vie for the lock when it is released even though only one client can acquire the lock.

### Advanced Exclusive Lock
Advanced Exclusive lock, which is corresponding to Simple Lock **WITHOUT** Herd Effect in paper.
In this situation, each client obtains the lock in order of request arrival.

#### About Test
Run the client several times (such as 3 or 5), and you have corresponding number of clients. Every client instance will execute the following process:
- Create /exclusive-lock-advanced in zookeeper server if it does not exist.
- Sleep for an interval between [`SLEEP_INTERVAL_BASE`, `SLEEP_INTERVAL_BASE` + `SLEEP_INTERVAL_RANGE`].
- Create `/exclusive-lock-advanced/lock_xxx`.
- Try to acquire the lock.
- If success, then sleep for an interval between [`SLEEP_INTERVAL_BASE/2`, `SLEEP_INTERVAL_BASE/2` + `SLEEP_INTERVAL_RANGE/2`].
- one round test over, continue until reaching to NUM_ROUND times.

### ReadWrite Lock
ReadWrite (Shared) lock, which is corresponding to Read/Write Locks in paper.For a specific transaction, if it is write transaction, it's the same as Advanced Exclusion Lock. if it is read transaction, then it has to wait the client which executes write transaction and orders just before it.

#### About Test
Run the client several times (such as 3 or 5), and you have corresponding number of clients. Every client instance will execute the following process:
- Create /ReadWrite-lock in zookeeper server if it does not exist.
- Sleep for an interval between [`SLEEP_INTERVAL_BASE`, `SLEEP_INTERVAL_BASE` + `SLEEP_INTERVAL_RANGE`].
- Create `/ReadWrite-lock-r[w]/lock_xxx`.
- Try to acquire the lock.
- If success, then sleep for an interval between [`TASK_INTERVAL_BASE`, `TASK_INTERVAL_BASE` + `TASK_INTERVAL_RANGE`].
- one round test over, continue until reaching to `NUM_ROUND` times.
You can also test read and write transaction sets according to custom order. For example, test such a transaction set: [w, r, r, r, w, w, r, r, w]. See comment code in `testRandom()`.

## Leader Election
Leader Election, which is corresponding to the Group Membership in paper.

#### About Test
Run the client several times (such as 3 or 5), and you have corresponding number of clients. Every client instance will execute the following process:
- Create /leader-members in zookeeper server if it does not exist.
- Sleep for an interval between [`SLEEP_INTERVAL_BASE`, `SLEEP_INTERVAL_BASE` + `SLEEP_INTERVAL_RANGE`].
- Try to create /leader-members/leader.
- If success, then exit.
- Else block until being triggered watch event.

## Queue
Zookeeper also can be applied to distributed queue, which resembling Message Queue Middleware, such as ActiveMQ and Kafaka.

### FIFO Queue
Actually, its implementation is the same as the (Advanced) Exclusive Lock. So, see `com.qqzeng.zkprimitives.lock.ExclusiveLockAdv`.

### Double Barrier
DoubleBarrier, which is corresponding to Double Barrier in paper. In short, before the client does some work, it must enter the barrier, and wait until all clients do. Before the client try to leave the barrier after it finishes work, it has to wait until all clients do.

#### About Test
Run the client several times (such as 3 or 5), and you have corresponding number of clients. Every client instance will execute the following process:
- Create /queue-double-barrier in zookeeper server if it does not exist.
- Sleep for an interval between [`SLEEP_INTERVAL_BASE`, `SLEEP_INTERVAL_BASE` + `SLEEP_INTERVAL_RANGE`].
- Create /queue-double-barrier/node-i_xxx.
- Try to synchronize enter the barrier before doing some work.
- Sleep for an interval between [`TASK_INTERVAL_BASE`, `TASK_INTERVAL_BASE` + `TASK_INTERVAL_RANGE`].
- Try to synchronize leave the barrier after doing some work.
- one round test over, continue until reaching to `NUM_ROUND` times.

### Barrier
Actually, compared to Double Barrier, Barrier just abandoned the procedure of leaving the barrier when finishing work.

