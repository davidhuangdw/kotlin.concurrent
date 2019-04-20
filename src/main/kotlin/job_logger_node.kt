import distributed.TcpPubSubListener
import distributed.TcpPubSubServer
import kotlinx.coroutines.launch
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.runBlocking
import java.net.InetSocketAddress

fun main() = runBlocking(newSingleThreadContext("job_logger_node")){
    fun log(str: String) = println("log:${System.currentTimeMillis()}:$str")

    val pubSubPort = 9999
    val pubSubServer = InetSocketAddress(pubSubPort)
    val listener = TcpPubSubListener(pubSubServer)

    launch { listener.start() }

    val lifeCycles = "put,start,run,done".split(",")
    val jobTypes = "jobA,jobB".split(",")
    for(jobType in jobTypes)
        for(phase in lifeCycles)
            listener.subRemote("$jobType.$phase")

    listener.subscribeEveryLine { line ->
        val fields = line.split(TcpPubSubServer.DELIMITER)
        if(fields[0] == TcpPubSubServer.RECV){
            val channel = fields[1]
            val data = fields[2]
            log("$channel::$data")
        }
    }
}