import distributed.TcpPubSubServer
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    val port = 9999
    val server = TcpPubSubServer(port)
    server.start().join()
}
