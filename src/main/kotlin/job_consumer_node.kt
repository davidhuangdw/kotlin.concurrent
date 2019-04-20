import coroutine.*
import distributed.TcpPubSubListener
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.runBlocking
import java.net.InetSocketAddress
import kotlin.random.Random

fun main() = runBlocking(newSingleThreadContext("job_runner_node_${System.currentTimeMillis()}")){
    val workerLimit = 3

    val pubSubPort = 9999
    val pubSubServer = InetSocketAddress(pubSubPort)
    val listener = TcpPubSubListener(pubSubServer)

    launch { listener.start() }

    launch {
        // worker creator:
        var wid = 1
        val workerCreatorSema = Semaphore(workerLimit)
        val client = listener.client
        val addr = client.localAddress

        while (true) {
            workerCreatorSema.acquire()
            launch {
                val jobType = "jobA"
                val workerId = wid++

                val params = listener.getUniqueListenId()
                println("worker:$workerId:start ---" + "-".repeat(workerId))

                val item = listener.takeRemoteItem(jobType, params)

                println("worker:$workerId:fetched $jobType item $item ---" + "-".repeat(workerId))
                listener.pubRemote("$jobType.start", "$item start by $addr")

                repeat(1 + Random.nextInt(5)) {
                    delay(5_000)
                    println("worker:$workerId:running item $item part $it ---" + "-".repeat(workerId))
                    listener.pubRemote("$jobType.run", "$item run part $it by $addr")
                }

                listener.pubRemote("$jobType.done", "$item done by $addr\n")
                println("worker:$workerId:done item $item ---" + "-".repeat(workerId))
                workerCreatorSema.release()
            }
        }
    }.join()
}