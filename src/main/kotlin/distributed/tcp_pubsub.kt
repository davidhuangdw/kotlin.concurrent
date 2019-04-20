package distributed

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import coroutine.*
import kotlinx.coroutines.*
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.AsynchronousSocketChannel
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine



class TcpPubSubServer(val port: Int) {
    companion object {
        const val BUFFER_SIZE = 5 * 1024
        const val ACCEPT_SOCKETS_LIMIT = 1000
        const val DEF_QUE_SIZE = 30

        const val DELIMITER = "::"
        const val CMD_SUB = "sub"
        const val CMD_UNSUB = "unsub"
        const val CMD_PUB = "pub"
        const val CMD_PUT = "put"
        const val CMD_TAKE = "take"
        const val CMD_EXIT = "exit"

        const val ITEM_KEY = "item"
        const val PARMS_KEY = "params"
        const val SUCC = "---success"
        const val FAILED = "---failed"
        const val RECV = "===received"
        const val RECV_QUE = "===receivedQue"
        val MANUAL = """
            Welcome!
            ------------------------------------
            Usage:
            sub::{channel}              # subscribe to a channel
            unsub::{channel}            # unsubscribe to a channel
            pub::{channel}::{data}      # publish data to channel
            put::{queue}::{item}        # put item into the buffered queue
            take::{queue}::{params}     # take one item from the buffered queue
            exit                        # exit
            ------------------------------------

        """.trimIndent()
        val singleThreadDispatcher = SingleThreadDispatcher()
    }

    private val mapper = ObjectMapper()

    private val scope = CoroutineScope(singleThreadDispatcher)
    private val acceptSema = Semaphore(ACCEPT_SOCKETS_LIMIT)
    private lateinit var blocked: Continuation<Unit>

    private val channelListeners = mutableMapOf<String, MutableSet<AsynchronousSocketChannel>>()
    private val subscribedChannels = mutableMapOf<AsynchronousSocketChannel, MutableSet<String>>()
    private val queues = mutableMapOf<String, Chan<String>>()

    fun start() = scope.launch {
        val serverChannel = AsynchronousServerSocketChannel.open().bind(InetSocketAddress(port))

        acceptSema.acquire()
        serverChannel.accept<Unit>(null, simpleHandler { ch, _, self ->
            println("---accept ${ch.remoteAddress}")
            launch {
                acceptSema.acquire()
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
                    acceptSema.release()
                }
            }
        })
        suspendCoroutine<Unit> { blocked = it }         // block to listen connections
    }

    fun close(){
        blocked.resume(Unit)
        scope.cancel()
    }

    fun receiveFormatline(channelId:String, data: String) = "$RECV::$channelId::$data\n"
    fun receiveQueFormatline(queueId:String, data: String) = "$RECV_QUE::$queueId::$data\n"

    fun respondSucc(requester: AsynchronousSocketChannel, cmd: String, channelOrQue: String, data: String=""){
        requester.writeStr(listOf(SUCC, cmd, channelOrQue, data).joinToString(DELIMITER, postfix = "\n"))
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
            cmd == CMD_PUB && fields.size == 3 -> publish(fields[1], fields[2], socket)
            cmd == CMD_PUT && fields.size == 3 -> put(fields[1], fields[2], socket)
            cmd == CMD_TAKE && fields.size == 3 -> take(fields[1], fields[2], socket)
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

    fun put(queueId: String, item: String, sender: AsynchronousSocketChannel){
        scope.launch {
            getQue(queueId).send(item)
            respondSucc(sender, CMD_PUT, queueId, item)
        }
    }

    fun take(queueId: String, params: String, requester: AsynchronousSocketChannel){
        scope.launch {
            val item = getQue(queueId).receive()
            val json = mapper.writeValueAsString(mapOf(ITEM_KEY to item, PARMS_KEY to params))
            requester.asyncWriteStr(receiveQueFormatline(queueId, json))
            respondSucc(requester, CMD_TAKE, queueId, params)
        }
    }

    fun publish(channel: String, data: String, sender: AsynchronousSocketChannel){
        val sockets = channelListeners[channel]
        if(sockets == null) return

        val line = receiveFormatline(channel, data)

        scope.launch {
            sockets.map {
                launch { it.asyncWriteStr(line) }
            }.joinAll()
            respondSucc(sender, CMD_PUB, channel, data)
        }
    }

    fun subscribe(channel: String, socket: AsynchronousSocketChannel) {
        channelListeners.getOrPut(channel) { mutableSetOf() }.add(socket)
        subscribedChannels.getOrPut(socket){ mutableSetOf() }.add(channel)
        respondSucc(socket, CMD_SUB, channel)
    }

    fun unsubscribe(channel: String, socket: AsynchronousSocketChannel){
        channelListeners.getOrPut(channel){ mutableSetOf() }.remove(socket)
        subscribedChannels.getOrPut(socket){ mutableSetOf() }.remove(channel)
        respondSucc(socket, CMD_UNSUB, channel)
    }

    fun unsubscribeAll(socket: AsynchronousSocketChannel) {
        if(socket !in subscribedChannels) return
        subscribedChannels[socket]!!.forEach { channelListeners[it]!!.remove(socket) }
        subscribedChannels.remove(socket)
    }

    private fun getQue(queueId: String) = queues.getOrPut(queueId){Chan(DEF_QUE_SIZE)}
}


class TcpPubSubListener(val address: SocketAddress){
    companion object {
        const val DELIMITER = TcpPubSubServer.DELIMITER
        const val CMD_SUB = TcpPubSubServer.CMD_SUB
        const val CMD_UNSUB = TcpPubSubServer.CMD_UNSUB
        const val CMD_PUB = TcpPubSubServer.CMD_PUB
        const val CMD_PUT = TcpPubSubServer.CMD_PUT
        const val CMD_TAKE = TcpPubSubServer.CMD_TAKE
        const val SUCC = TcpPubSubServer.SUCC
        const val FAILED = TcpPubSubServer.FAILED
        const val RECV = TcpPubSubServer.RECV
        const val RECV_QUE = TcpPubSubServer.RECV_QUE
        const val ITEM_KEY = TcpPubSubServer.ITEM_KEY
        const val PARMS_KEY = TcpPubSubServer.PARMS_KEY

        const val EVENT_ID_KEY =  "event_id"
    }

    lateinit var client: AsynchronousSocketChannel
    private val idEvents = mutableMapOf<String, MutableMap<String, MutableList<(payload: String)->Unit>>>() // { eventName => {id => callbackList } }
    private val everylineSubScribers = mutableListOf<(payload: String)->Unit>()
    private val mapper = ObjectMapper()
    private var nextUniqueId = 0

    init { runBlocking { client = AsynchronousSocketChannel.open().asyncConnect(address) } }


    suspend fun start(){
        val readBufferSize  = 5_000
        client.asyncReadExausted(ByteBuffer.allocate(readBufferSize)) {line ->
            everylineSubScribers.forEach{ it(line) }

            val fields = line.split(DELIMITER)
            if(fields.size < 2) return@asyncReadExausted
            when{
                fields[0] == SUCC -> handleSuccResp(fields)
                fields[0] == RECV_QUE -> handleRecvQueueResp(fields)
            }
        }
    }

    fun getUniqueListenId() = (nextUniqueId ++).toString()

    fun subscribeIdEvent(event: String, eventId: String, callback: (payload: String)->Unit) =
        idEvents.getOrPut(event) { mutableMapOf() }
            .getOrPut(eventId) { mutableListOf() }
            .add(callback)
    fun clearIdEvent(event: String, eventId: String) = idEvents?.get(event)?.remove(eventId)

    fun subscribeEveryLine(callback: (payload: String)->Unit){ everylineSubScribers.add(callback) }

    fun subscribeSuccPut(queueId: String, item: String, callback: (payload: String)->Unit){
        // DEF_QUE_SIZE will help prevent flooding
        val event = composeEvent(SUCC, CMD_PUT, queueId)
        val eventId = item
        subscribeIdEvent(event, eventId, callback)
    }

    fun subscribeSuccTake(queueId: String, params: String, callback: (payload: String) -> Unit){
        val event = composeEvent(SUCC, CMD_TAKE, queueId)
        val eventId = params            // the whole params as eventId
        subscribeIdEvent(event, eventId, callback)
    }

    fun subscribeRecvQue(queueId: String, params: String, callback: (payload: String) -> Unit){
        val event = composeEvent(RECV_QUE, queueId)
        val eventId = params            // the whole params as eventId
        subscribeIdEvent(event, eventId, callback)
    }

    suspend fun putRemoteItemAndWaitSucc(queueId: String, processUniqueItem: String){       // ensure item is unique in current process
        val query = composeQuery(CMD_PUT, queueId, processUniqueItem)
        val sema = Semaphore(0)

        subscribeSuccPut(queueId, processUniqueItem){ sema.release() }
        client.asyncWriteStr(query)
        sema.acquire()
    }

    suspend fun takeRemoteItem(queueId: String, processUniqueParams: String): String{  //ensure params is unique in current process
        val query = composeQuery(CMD_TAKE, queueId, processUniqueParams)
        val sema = Semaphore(0)
        var resultItem: String? = null

        subscribeRecvQue(queueId, processUniqueParams){
            resultItem = it
            sema.release()
        }
        client.asyncWriteStr(query)
        sema.acquire()
        return resultItem!!
    }

    suspend fun pubRemote(channelId: String, data: String) = client.asyncWriteStr(composeQuery(CMD_PUB, channelId, data))
    suspend fun subRemote(channelId: String) = client.asyncWriteStr(composeQuery(CMD_SUB, channelId))


    private fun composeQuery(vararg fields: String) = fields.joinToString(DELIMITER, postfix = "\n")
    private fun composeEvent(vararg fields: String) = fields.joinToString(DELIMITER)

    private fun getEventCallbackList(event: String, eventId: String): List<(String)->Unit> =
        idEvents.getOrElse(event){ return emptyList() }
            .getOrElse(eventId){ return emptyList() }

    private fun notifyEvent(event: String, eventId: String, payload: String) =
        getEventCallbackList(event, eventId).forEach{ it(payload) }

    private fun notifyEventOnlyOnce(event: String, eventId: String, payload: String){
        notifyEvent(event, eventId, payload)
        clearIdEvent(event, eventId)
    }

    private fun handleSuccResp(fields: List<String>){
        val (event, data) = parseResp(fields) ?: return
        val eventId = data                  // data as id because listener knows the whole data
        notifyEventOnlyOnce(event, eventId, data)
    }

    private fun handleRecvQueueResp(fields: List<String>){
        val (event, dataStr) = parseResp(fields) ?: return

        val data = parseJson(dataStr)
        val payload = data?.get(ITEM_KEY)?.textValue()   // queue item as payload
        val eventId = data?.get(PARMS_KEY)?.textValue()  // whole params as eventId because listener knows the whole params

        if(payload != null && eventId != null)
            notifyEventOnlyOnce(event, eventId, payload)
    }


    private fun parseResp(fields: List<String>): Pair<String, String>?{
        val k = fields.size
        val event = fields.subList(0, k-1).joinToString(DELIMITER)  // prefixes as eventName
        return event to fields.last()
    }

    private fun parseJson(str: String): JsonNode?{
        return try{
            mapper.readTree(str)
        }catch (e: JsonProcessingException){
            return null
        }
    }
}