package coroutine

// Synchronization primitives among coroutines, based on shared mutable state
// !!!warning: Related coroutines should be under single-thread(e.g. single-thread Dispatcher),
//             because these primitives are done by shared variables without lock

import kotlinx.coroutines.*
import org.junit.Test
import java.util.*
import java.util.concurrent.Executors
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine


fun SingleThreadDispatcher() = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

class Condition{
    private val waitingList = LinkedList<Continuation<Unit>>()
    suspend fun await() = suspendCoroutine<Unit> { waitingList.add(it) }
    suspend fun awaitUntil(predicate: ()->Boolean){
        while(!predicate())
            await()
    }

    fun signal(){
        if(waitingList.isEmpty()) return
        val routine = waitingList.poll()
        routine.resume(Unit)
    }

    fun signalAll(){
        while(waitingList.isNotEmpty()) signal()
    }
}

class Semaphore(val slots: Int){
    private val cond = Condition()
    private var availables = slots
    suspend fun acquire(){
        cond.awaitUntil { availables > 0 }
        availables--
    }

    fun release(){
        availables ++
        cond.signal()
    }
}

// warning: The pool is unnecessary and Semaphore is enough to limit the number of concurrent usage of a resource,
//      because unlike os-thread coroutine has no cost of OS resource
class CoroutinesPool(val size: Int){
    companion object {
        val singleThreadDispatcher = SingleThreadDispatcher()
    }
    private val tasks = LinkedList<suspend (jobId: Int)->Unit>()
    private val cond = Condition()
    private val joinCond = Condition()
    private var done = 0

    val scope = CoroutineScope(singleThreadDispatcher)
    var noMoreTask = false

    lateinit var routines: List<Job>
    fun start() = scope.launch{
        routines = (0 until size).map{ routineId -> launch{
            while(true){
                cond.awaitUntil{ tasks.isNotEmpty() || noMoreTask}
                if(tasks.isEmpty()) break       // no more task
                val task = tasks.poll()
                task(routineId)
            }
            done ++
            if(done == size) joinCond.signalAll()
        } }
    }

    fun schedule(task: suspend (routineId: Int)->Unit ){
        tasks.add(task)
        cond.signal()
    }

    fun shutdown(){
        noMoreTask = true
        cond.signalAll()
    }

    suspend fun join() = joinCond.awaitUntil { done == size }
}

class FairReadWriteSemaphore{   // for NIO multiple reads and single write
    var readCount = 0
    private val waitingList = LinkedList<Pair<Boolean, Continuation<Unit>>>() //

    suspend fun read_acquire(){
        if(readCount >= 0 && waitingList.isEmpty())
            readCount ++
        else
            await(true)
    }
    fun read_release(){
        readCount --
        if(readCount == 0) signal()
    }

    suspend fun write_acquire(){
        if(readCount == 0 && waitingList.isEmpty())
            readCount --
        else
            await(false)
    }
    fun write_release(){
        readCount ++
        if(readCount == 0) signal()
    }

    private suspend fun await(isRead: Boolean) = suspendCoroutine<Unit> { waitingList.add(isRead to it) }
    private fun signal(){
        if(waitingList.isEmpty()) return
        val (isRead, cont) = waitingList.poll()
        readCount += if(isRead) 1 else -1
        if(!isRead) return cont.resume(Unit)

        val readers = mutableListOf(cont)
        while(waitingList.isNotEmpty()){
            val (isRead, cont) = waitingList.peek()
            if(!isRead) break
            waitingList.poll()
            readCount ++
            readers.add(cont)
        }
        readers.forEach{ it.resume(Unit) }
    }

}

open class Chan<T>(val bufferSize: Int = Int.MAX_VALUE){
    private val sendersCompeteSema = Semaphore(bufferSize)
    private val sendSema = Semaphore(0)
    private val recvSema = Semaphore(0)
    private var sents = LinkedList<T>()
    private val emptyCond = Condition()

    fun size() = sents.size

    suspend fun send(x: T){
        sendersCompeteSema.acquire()
        sents.add(x)
        recvSema.release()
    }

    suspend fun receive(): T{
        recvSema.acquire()
        return sents.poll()!!.also {
            if(sents.isEmpty()) emptyCond.signal()
            sendersCompeteSema.release()
        }
    }

    suspend fun waitTillEmpty() = emptyCond.awaitUntil { sents.isEmpty() }
}

class UnbufferedChan<T>: Chan<T>(1)

class PrimitivesTests{
    val dispatcher = SingleThreadDispatcher()
    val scope = CoroutineScope(dispatcher)

    @Test
    fun testSemaphore() = runBlocking {
        val sema = Semaphore(3)
        (0 until 50).map{ id -> scope.launch{
            sema.acquire()
            println("acquire:$id: ---" + "-".repeat(id))
            delay(200L)
            println("release:$id: ===" + "=".repeat(id))
            sema.release()

        } }.forEach{ it.join() }
        println("complete testSemaphore")
    }

    @Test
    fun testCoroutinesPool() = runBlocking {
        val pool = CoroutinesPool(5)
        pool.start()
        (0 until 20).forEach { taskId ->
            pool.schedule { coroutineId ->
                println("$coroutineId: task $taskId started " + "-".repeat(taskId))
                delay(4000L)
                println("$coroutineId: task $taskId done " + "=".repeat(taskId))
            }
        }

        launch {
            delay(1_000)
            pool.shutdown()
        }

        pool.join()
        println("complete testCoroutinesPool")
    }

    @Test
    fun testFairReadWriteSemaphore() = runBlocking {
        val sema = FairReadWriteSemaphore()
        (0 until 50).map{id -> scope.launch {         // single-thread
            if((id/8)%2 == 0){
                sema.read_acquire()
                println("read $id: ${sema.readCount} -" + "-".repeat(id))
                delay(100L)
                sema.read_release()
            }else{
                sema.write_acquire()
                println("write $id: ${sema.readCount} =" + "=".repeat(id))
                delay(100L)
                sema.write_release()
            }
        } }.forEach{ it.join() }
        println("complete testFairReadWriteSemaphore")
    }

    @Test
    fun testChan() = runBlocking {
        listOf(1,3).forEach { bufferSize ->
            println("=============buffer size $bufferSize==================")
            val chan = Chan<Int>(bufferSize)
            scope.launch {
                while(true){
                    delay(bufferSize * 500L)
                    println("===received: ${chan.receive()}")
                }
            }
            scope.launch {
                (0 until 10).forEach {
                    delay(it*200L)
                    chan.send(it)
                    println("---sent: $it")
                }
            }.join()
            chan.waitTillEmpty()
        }
    }

}
