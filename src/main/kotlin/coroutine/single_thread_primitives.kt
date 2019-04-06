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


class PrimitivesTests{
    val dispatcher = SingleThreadDispatcher()
    val scope = CoroutineScope(dispatcher)

    @Test
    fun testSemaphore() = runBlocking {
        val sema = Semaphore(3)
        repeat(50){ id -> scope.launch{
                sema.acquire()
                println("acquire:$id: ---" + "-".repeat(id))
                delay(200L)
                println("release:$id: ===" + "=".repeat(id))
                sema.release()

        } }
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
        println("complete all")
    }

}