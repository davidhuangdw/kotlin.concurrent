package coroutine

import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.CompletionHandler
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine


fun <V, A> simpleHandler(onComplete: (result: V, attachment: A?, self: CompletionHandler<V, A>) -> Unit) =
    object : CompletionHandler<V, A> {
        override fun completed(result: V, attachment: A?) = onComplete(result, attachment, this)
        override fun failed(exc: Throwable?, attachment: A?) = throw exc!!
    }

suspend fun AsynchronousSocketChannel.asyncConnect(address: SocketAddress): AsynchronousSocketChannel {
    suspendCoroutine<Unit> { cont ->
        this.connect(address, null, simpleHandler { _, _, self -> cont.resume(Unit) })
    }
    return this
}

suspend fun AsynchronousSocketChannel.asyncWrite(buffer: ByteBuffer): Int = suspendCoroutine { cont ->
    this.write(buffer, null, simpleHandler { writeSize, _, _ -> cont.resume(writeSize) })
}

suspend fun AsynchronousSocketChannel.asyncRead(buffer: ByteBuffer): Int  = suspendCoroutine { cont ->
    this.read(buffer, null, simpleHandler { readSize, _, _ -> cont.resume(readSize) })
}

suspend fun AsynchronousSocketChannel.asyncReadExausted(buffer: ByteBuffer, readLine: (line: String) -> Unit) {
    while (true) {
        val readSize = asyncRead(buffer)
        if (readSize < 0) break
        readBuffer(buffer).forEach(readLine)
    }
}

fun AsynchronousSocketChannel.writeStr(str: String) = write(bufferStr(str))
suspend fun AsynchronousSocketChannel.asyncWriteStr(str: String) = asyncWrite(bufferStr(str))

fun readBuffer(buffer: ByteBuffer): List<String> {
    buffer.flip()
    val size = buffer.limit()
    return String(ByteArray(size).also {
        buffer.get(it, 0, size).clear()
    }).lines().dropLast(1)
}

fun bufferStr(str: String) = ByteBuffer.wrap(str.toByteArray())
