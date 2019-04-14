package examples

import coroutine.Chan
import kotlinx.coroutines.*
import multi_threads.AtomInt
import multi_threads.ConcurrentDeque
import multi_threads.Semaph
import org.junit.Test
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import kotlin.concurrent.withLock

abstract class SleepingBarberSimulator(val seats: Int){
    companion object {
        const val BARBER_PREP_SEC = 3
        const val BARBER_WORK_SEC = 8
        const val BARBER_REST_SEC = 3

        const val BARBER_WORK_VAR = 2
    }
    private val rand = Random()

    abstract fun newBarber()
    abstract fun newCustomer()
    abstract fun joinAllCustomers()

    protected fun logBarber(msg: String) = println("=".repeat(100)+"Barber "+msg)
    protected fun barberWorkSpeed() = BARBER_WORK_SEC - BARBER_WORK_VAR + rand.nextInt(BARBER_WORK_VAR*2)

}

class MultiThreadSleepingBarberSimulator(seats: Int): SleepingBarberSimulator(seats){
    private val bid = AtomInt()
    private val cid = AtomInt()
    private val steps = (0 until 10).map{ Semaph(0) }

    private val freeSeats = AtomInt(seats)
    private val repliedCustomers = ConcurrentDeque<Int>()
//    val repliedCustomers = ConcurrentLinkedDeque<Int>()

    private val barberCustomerSemas = Hashtable<Int, Pair<Semaph, Semaph>>()
    val customerThreads = LinkedList<Thread>()

    override fun newBarber(){
        val id = bid.inc()
        val workSpeed = barberWorkSpeed()
        Thread{
            logBarber("$id comes and make some prepare")
            sleep(BARBER_PREP_SEC)

            while(true){
                // steps[0].acquire()
                steps[0].lock.withLock {
                    val s = steps[0]
                    var slept = false
                    if(s.count == 0) {
                        slept = true
                        logBarber("$id sleep since no customer")
                        s.cond.await()
                    }
                    s.count --
                    if(slept) logBarber("$id awake")
                }
                logBarber("$id asking for a customer")
                steps[1].release()

                steps[2].acquire()
                val customer = repliedCustomers.poll()
                logBarber("$id working on customer $customer")
                sleep(workSpeed)
                logBarber("$id done on customer $customer")
                val (cs, bs) = barberCustomerSemas[customer]!!
                cs.release()

                bs.acquire()
                logBarber("$id has a rest")
                sleep(BARBER_REST_SEC)
                barberCustomerSemas.remove(customer)
            }
        }.apply{ start() }
    }

    override fun newCustomer() {
        val id = cid.inc()
        customerThreads.add(Thread{
            println("---Customer $id comes")
            if(freeSeats.dec() < 0){
                println("!!!!!!!!!!!!Customer $id has no seat and leaves")
                freeSeats.inc()
                return@Thread
            }
            println("---Customer $id sits and waits")
            steps[0].release()

            steps[1].acquire()
            println("---Customer $id responds, stands up and is served")
            freeSeats.inc()
            repliedCustomers.add(id)
            barberCustomerSemas[id] = Semaph(0) to Semaph(0)
            steps[2].release()

            val (cs, bs) = barberCustomerSemas[id]!!
            cs.acquire()
            println("---Customer $id done and leaves")
            bs.release()
        }.apply{ start() })
    }
    override fun joinAllCustomers() = customerThreads.forEach{ it.join() }
    private fun sleep(nSecond: Int) = Thread.sleep(nSecond * 1000L)
}

class CoroutineSleepingBarberSimulator(seats: Int): SleepingBarberSimulator(seats){     // by single thread coroutine
    var bid = 0
    var cid = 0
    val scope = CoroutineScope(newSingleThreadContext("sleep barbers"))
    val customerJobs = LinkedList<Job>()

    val seatedCustomers = Chan<Int>()
    val customerChans = mutableMapOf<Int, Chan<Int>>()
    val barberChans = mutableMapOf<Int, Chan<Int>>()
    override fun newBarber(){
        val id = ++bid
        val workSpeed = barberWorkSpeed()
        scope.launch {
            logBarber("$id comes and make some prepare")
            sleep(BARBER_PREP_SEC)
            while(true){
                val customer = seatedCustomers.receive()
                val customerChan = customerChans[customer]!!
                val chan = Chan<Int>(1)
                barberChans[id] = chan
                logBarber("$id asking for a customer")
                customerChan.send(id)

                chan.receive()
                logBarber("$id working on customer $customer")
                sleep(workSpeed)
                logBarber("$id done on customer $customer")
                customerChan.send(0)

                chan.receive()
                logBarber("$id has a rest")
                sleep(BARBER_REST_SEC)
                barberChans.remove(id)
            }
        }
    }
    override fun newCustomer(){
        val id = ++cid
        customerJobs.add(scope.launch {
            println("---Customer $id comes")
            if(seatedCustomers.size() >= seats){
                println("!!!!!!!!!!!!Customer $id has no seat and leaves")
                return@launch
            }
            println("---Customer $id sits and waits")
            val chan =  Chan<Int>(1)
            customerChans[id] = chan
            seatedCustomers.send(id)

            val barber = chan.receive()
            val barberChan = barberChans[barber]!!
            println("---Customer $id responds, stands up and is served by barber $barber")
            barberChan.send(0)

            chan.receive()
            println("---Customer $id done and leaves")
            customerChans.remove(id)
            barberChan.send(0)
        })
    }
    override fun joinAllCustomers() = runBlocking { customerJobs.forEach { it.join() } }
    private suspend fun sleep(nSecond: Int) = delay(nSecond * 1000L)
}

class SleepingBarbersTests{
    val seats = 3
    val barbersNum = 2
    val roundNum = 2
    val roundInterval = 30_000
    val roundCustomersNum = 15
    val customerInterval = 5000
    val rand = Random()

    @Test
    fun testMultiThreadSleepingBarbers(){
        val sim = MultiThreadSleepingBarberSimulator(seats)
        repeat(barbersNum) { sim.newBarber() }
        repeat(roundNum){
            repeat(roundCustomersNum){
                Thread.sleep(rand.nextInt(customerInterval).toLong())

                sim.newCustomer()
            }
            Thread.sleep(roundInterval.toLong())
        }
        sim.joinAllCustomers()
    }

    @Test
    fun testCoroutineSleepingBarberSimulator(){
        val sim = CoroutineSleepingBarberSimulator(seats)
        repeat(barbersNum) { sim.newBarber() }
        repeat(roundNum){
            repeat(roundCustomersNum){
                Thread.sleep(rand.nextInt(customerInterval).toLong())

                sim.newCustomer()
            }
            Thread.sleep(roundInterval.toLong())
        }
        sim.joinAllCustomers()

    }
}
