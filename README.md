# Kotlin Concurrent Demos

1. multi-thread primitives: 
    * AtomicInt, SpinLock, ReentrantSpinLock
    * ConcurrentDeque, BlockingLock, Semaphore, CycleBarrier
    * ReaderWriterLock, FutureTask, ThreadPool

2. coroutine primitives:
    * (based on shared mutable state under single-thread)
    * Condition, Semaphore, CoroutinePool, FairReadWriteSemaphore
    * buffered/unbuffered/unlimited Channel
    
3. examples: 
    * multiple barbers problem by multi-thread and coroutine
    * A TCP pub_sub and simple message queue server by coroutine
    * A distributed job dispatch system by coroutine
        * a message queue server, a logger node, and many-to-many producer nodes and consumer nodes
        * enable the throughput control on both producer and consumer sides
            * consumer side: suspend on taking a job from message_queue server, and use a semaphor to control how many coroutines to take job concurrently
            * producer side: use the buffered Channel on message_queue server, which will suspend send ACK to producer node when reach buffer limit
