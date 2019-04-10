import coroutine.Semaphore
import coroutine.SingleThreadDispatcher
import kotlinx.coroutines.*
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.CompletionHandler
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine



fun <V, A> simpleHandler(onComplete: (result: V, attachment: A?, self: CompletionHandler<V, A>) -> Unit) =
    object : CompletionHandler<V, A> {
        override fun completed(result: V, attachment: A?) = onComplete(result, attachment, this)
        override fun failed(exc: Throwable?, attachment: A?) = throw exc!!
    }

suspend fun AsynchronousSocketChannel.asyncRead(buffer: ByteBuffer): Int {
    return suspendCoroutine { cont ->
        this.read(buffer, null, simpleHandler { readSize, _, _ ->
            cont.resume(readSize)
        })
    }
}
fun AsynchronousSocketChannel.writeStr(str: String) = write(bufferStr(str))

fun readBuffer(buffer: ByteBuffer): List<String> {
    buffer.flip()
    val size = buffer.limit()
    return String(ByteArray(size).also {
        buffer.get(it, 0, size).clear()
    }).lines().dropLast(1)
}

fun bufferStr(str: String) = ByteBuffer.wrap(str.toByteArray())

class TcpPubSub {
    companion object {
        const val BUFFER_SIZE = 5 * 1024
        const val CONCURRENT_LIMIT = 100
        const val DELIMITER = "::"
        const val CMD_SUB = "sub"
        const val CMD_UNSUB = "unsub"
        const val CMD_PUB = "pub"
        const val CMD_EXIT = "exit"
        val MANUAL = """
            Welcome!
            ------------------------------------
            Usage:
            sub::{channel}              # subscribe to a channel
            unsub::{channel}            # unsubscribe to a channel
            pub::{channel}::{data}      # publish data to channel
            exit                        # exit
            ------------------------------------

        """.trimIndent()
        val singleThreadDispatcher = SingleThreadDispatcher()
    }
    private val scope = CoroutineScope(singleThreadDispatcher)
    private val sema = Semaphore(CONCURRENT_LIMIT)
    private lateinit var blocked: Continuation<Unit>

    private val channelSockets = mutableMapOf<String, MutableSet<AsynchronousSocketChannel>>()
    private val channels = mutableMapOf<AsynchronousSocketChannel, MutableSet<String>>()

    fun start() = scope.launch {
        val serverChannel = AsynchronousServerSocketChannel.open().bind(InetSocketAddress(9999))

        sema.acquire()
        serverChannel.accept<Unit>(null, simpleHandler { ch, _, self ->
            println("---accept ${ch.remoteAddress}")
            launch {
                sema.acquire()
                serverChannel.accept(null, self)
            }
            launch {
                try {
                    ch.writeStr(MANUAL)
                    val buffer = ByteBuffer.allocate(BUFFER_SIZE)

                    var done = false
                    while (!done) {
                        val readSize = ch.asyncRead(buffer)
                        if (readSize < 0) break
                        readBuffer(buffer).forEach { done = handleRequest(it, ch) }
                    }
                    println("---${ch.remoteAddress}: done")
                } finally {
                    clearSocket(ch)
                    sema.release()
                }
            }
        })
        suspendCoroutine<Unit> { blocked = it }         // block to listen connections
    }

    fun close(){
        blocked.resume(Unit)
        scope.cancel()
    }

    fun clearSocket(socket: AsynchronousSocketChannel){
        unsubscribeAll(socket)
        socket.close()
    }

    fun handleRequest(line: String, socket: AsynchronousSocketChannel): Boolean {
        println("${socket.remoteAddress}: $line")
        val fields = line.split(DELIMITER)
        val cmd = fields[0]
        when {
            cmd == CMD_EXIT -> return true
            cmd == CMD_PUB && fields.size == 3 -> publish(fields[1], fields[2])
            cmd == CMD_SUB && fields.size == 2 -> subscribe(fields[1], socket)
            cmd == CMD_UNSUB && fields.size == 2 -> unsubscribe(fields[1], socket)
            else -> {
                val error = "${socket.remoteAddress}:unknown command or wrong format: $line\n"
                print(error)
                scope.launch { socket.writeStr(error) }
            }
        }
        return false
    }

    fun publish(channel: String, data: String){
        val sockets = channelSockets[channel]
        if(sockets == null) return

        val line = "===received: $data\n"
        scope.launch { sockets.forEach { it.writeStr(line) } }
    }

    fun subscribe(channel: String, socket: AsynchronousSocketChannel) {
        channelSockets.getOrPut(channel) { mutableSetOf() }.add(socket)
        channels.getOrPut(socket){ mutableSetOf() }.add(channel)
    }

    fun unsubscribe(channel: String, socket: AsynchronousSocketChannel){
        channelSockets.getOrPut(channel){ mutableSetOf() }.remove(socket)
        channels.getOrPut(socket){ mutableSetOf() }.remove(channel)
    }

    fun unsubscribeAll(socket: AsynchronousSocketChannel) {
        if(socket !in channels) return
        channels[socket]!!.forEach { channelSockets[it]!!.remove(socket) }
        channels.remove(socket)
    }
}

fun main(){
    val server = TcpPubSub()
    val job =server.start()
    runBlocking { job.join() }
}
