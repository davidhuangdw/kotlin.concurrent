package multi_threads

import org.junit.Test
import sun.misc.Unsafe
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

const val UNLOCKED = 0
const val LOCKED = 1

interface Lock {
    fun tryLock(): Boolean
    fun unlock()
}

fun Lock.lock(){ while(!tryLock()); }
fun Lock.withLock(blk: () -> Any?): Any? {
    try {
        lock()
        return blk()
    } finally {
        unlock()
    }
}

open class AtomInt(@Volatile var atomInt: Int = 0) {
    companion object {
        private fun getTheUnsafe(): Unsafe {
            val field = Unsafe::class.java.getDeclaredField("theUnsafe")
            field.isAccessible = true
            return field.get(null) as Unsafe
        }

        val unsafe = getTheUnsafe()
        val atomIntOffset = unsafe.objectFieldOffset(AtomInt::class.java.getDeclaredField("atomInt"))
    }

    fun cas(expect: Int, setValue: Int) = unsafe.compareAndSwapInt(this,
        atomIntOffset, expect, setValue)

    fun add(delta: Int): Int {
        var v = atomInt
        while (!cas(v, v + delta))
            v = atomInt
        return v + delta
    }

    fun inc() = add(1)
    fun dec() = add(-1)

    fun get() = atomInt
    fun set(v: Int) {
        atomInt = v
    }
}

open class SpinLock : AtomInt(UNLOCKED), Lock {
    override fun tryLock() = cas(UNLOCKED, LOCKED)
    override fun unlock() { set(UNLOCKED) }
}

class TicketSpinLock {
    private val nTicket = AtomInt(0)
    private val current = AtomInt(1)

    fun getTicket() = nTicket.inc()
    fun lock(ticket: Int) {
        while (current.get() != ticket);
    }

    fun unlock(ticket: Int) = current.cas(ticket, ticket + 1)
    fun withLock(ticket: Int, blk: () -> Any?): Any? {
        try {
            lock(ticket)
            return blk()
        } finally {
            unlock(ticket)
        }
    }
}

class ReentrantSpinLock: Lock {
    @Volatile
    var owner: Thread? = null
    val count = AtomInt()

    // owner != null => count > 0, count==0 => owner == null
    override fun tryLock(): Boolean {
        val current = Thread.currentThread()
        if(current == owner) {
            count.inc()
            return true
        }
        return count.cas(0, 1).also{
            if(it) owner = current
        }
    }

    override fun unlock() { // only currentThread is able to unlock, so no parallel with other 'count change'
        if(count.get() == 1) owner = null
        count.set(count.get()-1)        // without cas
    }
}

class ConcurrentDeque<T> {
    private val lock = SpinLock()
    val innerQue = LinkedList<T>()      // potential error: 'next/prev' is not volatile inside each node
    fun withLock(blk: () -> Any?) = lock.withLock(blk)
    fun add(v: T) = withLock { innerQue.add(v) }
    fun poll() = withLock { if (innerQue.isEmpty()) null else innerQue.poll() } as T?
    fun pollAll(blk: (T) -> Any?) = withLock {
        while (innerQue.isNotEmpty()) blk(innerQue.poll())
    }

    fun size() = innerQue.size
}

//class Cond {
//    private val waitList = ConcurrentDeque<Thread>()
//
//    fun size() = waitList.size()
//
//    fun await() {
//        waitList.add(Thread.currentThread())
//        // error: potentially wait forever!!
//        //  e.g. the order: 1. enqueue and not park yet 2. poll and unpark 3. park forever because unpark has done
//        // problem: impossible to use LockSupport.park directly with my own SpinLock
//        // seems impossible without ReentrantLock#newCondition:
//        //     because seems unable to fetch the binding mutex of an Object or currentThread
//        LockSupport.park(this)
//    }
//
//    fun signal() {
//        val first = waitList.poll()
//        if (first != null)
//            LockSupport.unpark(first)
//    }
//
//    fun signalAll() = waitList.pollAll { LockSupport.unpark(it) }
//}

open class BlockingLock {
    val lock = ReentrantLock()
    val cond = lock.newCondition()
    val state = ReentrantSpinLock()

    open fun lock() {
        lock.withLock {
            while (!state.tryLock())
                cond.await()
        }
    }

    open fun unlock() {
        state.unlock()
        lock.withLock { cond.signal() }
    }

    open fun unlockRandom() {
        state.unlock()
        lock.withLock { cond.signalAll() }
    }
}

class Semaph(slots: Int) {
    @Volatile
    var count = slots
    var lock = ReentrantLock()
    val cond = lock.newCondition()

    fun tryAcquire(k: Int=1) = lock.withLock {
        (count >= k).also{
            if(it) count -= k
            else cond.await()
        }
    }

    fun acquire(k: Int = 1) { while(!tryAcquire(k)); }
    fun release() = lock.withLock {
        count ++
        cond.signal()
    }

    fun releaseK(k: Int = 1) = lock.withLock {
        count += k
        cond.signalAll()
    }
}

class CycleBarrier(val round_size: Int){
    private val count = AtomInt(0)
    private val lock = ReentrantLock()
    private val cond = lock.newCondition()

    fun await(){
        val id = count.inc()
        val round = (id - 1)/round_size

        lock.withLock {
            if(id % round_size == 0) cond.signalAll()
            while(count.get()/round_size <= round)
                cond.await()
        }
    }
}

class BlockingQue<T>(val limit: Int = Int.MAX_VALUE) {
    private val lock_take = ReentrantLock()
    private val cond_take = lock_take.newCondition()
    private val lock_put = ReentrantLock()
    private val cond_put = lock_put.newCondition()
    val que = ConcurrentDeque<T>()

    fun size() = que.size()

    fun take(after_take: (T) -> Unit = {}): T {
        var res = que.poll()
        lock_take.withLock {
            while(res == null) {
                cond_take.await()
                res = que.poll()
            }
        }
        after_take(res!!)
        lock_put.withLock { cond_put.signal() }
        return res!!
    }

    fun put(x: T, after_put: () -> Unit = {}) {
        lock_put.withLock{
            while (true) {
                val succeed = que.withLock {
                    (que.innerQue.size < limit).also {
                        if (it) que.innerQue.add(x)
                    }
                } as Boolean
                if (succeed) break
                cond_put.await()
            }
        }
        after_put()
        lock_take.withLock{ cond_take.signal() }
    }
}

class ReaderWriterLock {
    @Volatile
    var nwrite = 0
    @Volatile
    var nread = 0
    private val lock = ReentrantLock()
    private val read_cond = lock.newCondition()
    private val write_cond = lock.newCondition()

    fun read_acquire() {
        lock.withLock {
            while (nwrite > 0)
                read_cond.await()
            nread ++
        }
    }

    fun read_release() {
        lock.withLock {
            nread--
            if(nread == 0) write_cond.signal()
        }
    }

    fun write_acquire() {
        lock.withLock {
            while(!(nread == 0 && nwrite == 0))
                write_cond.await()
            nwrite ++
        }
    }

    fun write_release() {
        lock.withLock {
            nwrite --
            write_cond.signalAll()
            read_cond.signalAll()
        }
    }
}

class FutTask<T>(val task: Callable<T>): Runnable{
    @Volatile
    var result: T? = null
    @Volatile
    var done = false
    private val lock = ReentrantLock()
    private val cond = lock.newCondition()

    fun get(): T{
        lock.withLock {
            while(!done)
                cond.await()
        }
        return result!!
    }

    override fun run(){
        val ret = task.call()
        lock.withLock {
            done = true
            result = ret
            cond.signalAll()
        }
    }
}

class ThreadPool(val size: Int) {
    @Volatile
    var finished = false
    val tasks = ConcurrentDeque<Runnable>()
    val lock = ReentrantLock()
    val cond = lock.newCondition()

    val workers = (0 until size).map {
        Thread{
            while(true){
                var task = tasks.poll()
                lock.withLock {
                    while(task == null && !finished) {
                        cond.await()
                        task = tasks.poll()
                    }
                }
                if(task == null && finished) break
                task!!.run()
            }
        }.apply { start() }
    }

    fun add(task: Runnable) {
        tasks.add(task)
        lock.withLock { cond.signal() }
    }

    fun finish(){
        finished = true
        lock.withLock { cond.signalAll() }
    }
    fun join() = workers.map{ it.join() }
}


class MultiThreadsTests{
    fun threadsRepeatRun(nThreads: Int, repeats: Int=0, task: (id: Int, index: Int) -> Unit){
        class Task(val id: Int): Runnable{
            override fun run(){
                (0 until repeats).forEach{
                    task(id, it)
                }
            }
        }
        (0 until nThreads).map{
            Thread(Task(it)).apply{ start() }
        }.map{ it.join() }
    }

    @Test
    fun testAtomInt(){
        val atom = AtomInt()
        threadsRepeatRun(30, 10){ id, _ ->
            println("${id}: \t${atom.add(1)}")
        }
        println(atom.get())
    }

    @Test
    fun testSpinLock(){
        val slock = SpinLock()
        threadsRepeatRun(20, 1){ id, _ ->
            slock.withLock {
                println("${id}: locked ---" + "-".repeat(id))
                Thread.sleep(if(id==0) 1000L else 100L)
                println("${id}: unlocked ===" + "=".repeat(id))
            }
        }
    }

    @Test
    fun testTicketLock(){
        val tlock = TicketSpinLock()
        threadsRepeatRun(20, 1){ id, _ ->
            val ticket = tlock.getTicket()
            tlock.withLock(ticket){
                println("${id}: locked ---" + "-".repeat(id))
                Thread.sleep(if(id==0) 1000L else 100L)
                println("${id}: unlocked ===" + "=".repeat(id))
            }
        }
    }

    @Test
    fun testReentrantLock(){
        val rLock = ReentrantSpinLock()
        threadsRepeatRun(50, 1){id, _ ->
            val cnt = 3
            for (i in 0 until cnt) {
                rLock.lock()
                println("${id}: locked $i ---" + "-".repeat(id))
            }
            Thread.sleep(if(id==0) 1000L else 100L)
            for (i in cnt-1 downTo 0) {
                println("${id}: unlocked $i ===" + "=".repeat(id))
                rLock.unlock()
            }
        }
    }

    @Test
    fun testBlockingLock(){
        val bLock = BlockingLock()
        threadsRepeatRun(50, 1){id, _ ->
            bLock.lock()
            println("${id}: locked ---" + "-".repeat(id))
            Thread.sleep(if(id==0) 1000L else 100L)
            println("${id}: unlocked ===" + "=".repeat(id))
            bLock.unlockRandom()
        }
    }


    @Test
    fun testSemaph(){
        val sema = Semaph(3)
        threadsRepeatRun(50, 1) { id, _ ->
            sema.acquire()
            println("acquire:$id: ---" + "-".repeat(id))
            Thread.sleep(200L)
            println("release:$id: ===" + "=".repeat(id))
            sema.release()
        }
    }

    @Test
    fun testCycleBarrier(){
        val round_size = 4
        val barr = CycleBarrier(round_size)
        for(r in 0 until 2){
            println("---------- round $r")
            threadsRepeatRun(round_size, 1) { id, _ ->
                println("$id: begin $id ---" + "-".repeat(id))
                if(id % round_size < 2) Thread.sleep(2000)
                barr.await()
                println("$id:   end $id ===" + "=".repeat(id))
            }
        }
    }

    @Test
    fun testBlockingQue(){
        val bQue = BlockingQue<Int>(3)
        threadsRepeatRun(50, 1) { id, _ ->
            if(id < 25){
                bQue.put(id){
                    println("producer:$id: ${bQue.size()} put $id ---" + "-".repeat(id))
                }
            }else{
                bQue.take{
                    println("consumer:$id: ${bQue.size()} take $it---" + "-".repeat(id))
                }
            }
        }
        println(bQue.size())
    }

    @Test
    fun testReaderWriterLock(){
        val rwLock = ReaderWriterLock()
        threadsRepeatRun(50, 1) { id, _ ->
            if((id/10)%2 == 1){
                rwLock.read_acquire()
                println("read:$id: (${rwLock.nread}, ${rwLock.nwrite}) ---" + "-".repeat(id))
                Thread.sleep(100)
                rwLock.read_release()
            }else{
                rwLock.write_acquire()
                println("write:$id: (${rwLock.nread}, ${rwLock.nwrite}) ---" + "-".repeat(id))
                rwLock.write_release()
            }
        }
    }

    @Test
    fun testThreadPool(){
        val pool = ThreadPool(5)
        val ntasks = 30
        (0 until ntasks).map{
            val task = Runnable{
                println("task:$it: start ---" + "-".repeat(it))
                Thread.sleep(3000)
                println("task:$it:   end ===" + "=".repeat(it))
            }
            pool.add(task)
        }

        pool.finish()
        pool.join()
    }

    @Test
    fun testFutTask(){
        val pool = ThreadPool(2)
        val fa = FutTask<Int>(Callable {
            println("fa start---")
            Thread.sleep(3000)
            println("fa done---")
            100
        })
        val fb = FutTask<Int>(Callable {
            println("fb start---")
            Thread.sleep(3000)
            println("fb done---")
            200
        })
        val fsum = FutTask<Int>(Callable {
            val sum = fa.get() + fb.get()
            println("fsum done---")
            sum
        })

        for(task in listOf(fa, fb, fsum))
            pool.add(task)
        pool.finish()

        println(fsum.get())
        pool.join()
    }

}
