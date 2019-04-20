import distributed.TcpPubSubListener
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.runBlocking
import java.net.InetSocketAddress
import kotlin.random.Random

fun main() = runBlocking(newSingleThreadContext("job_producer_node_${System.currentTimeMillis()}")){
    val pubSubPort = 9999
    val pubSubServer = InetSocketAddress(pubSubPort)
    val listener = TcpPubSubListener(pubSubServer)

    launch { listener.start() }

    launch {
        val jobTypes = "jobA,jobB".split(",")
        for(jobType in jobTypes) launch {
            repeat(100){
                delay(Random.nextLong(10_000))
                val item = jobType + it.toString()

                println("produce $jobType with content of $item")
                listener.putRemoteItemAndWaitSucc(jobType, item)
            }
        }
    }.join()
}