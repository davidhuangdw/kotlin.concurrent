package examples

import multi_threads.AtomInt
import multi_threads.ConcurrentDeque
import multi_threads.Semaph
import org.junit.Test
import java.util.*


class MultiThreadSleepingBarberSimulator(seats: Int){
    companion object {
        const val BARBER_PREP_SEC = 3
        const val BARBER_WORK_SEC = 8
        const val BARBER_REST_SEC = 3
    }
    val steps = (0 until 10).map{ Semaph(0) }

    val bid = AtomInt()
    val cid = AtomInt()

    val freeSeats = AtomInt(seats)
    val repliedCustomers = ConcurrentDeque<Int>()

    fun newBarber(){
        val id = bid.inc()
        Thread{
            logBarber("$id comes and make some prepare")
            sleep(BARBER_PREP_SEC)

            while(true){
                steps[0].acquire()
                logBarber("$id asking for a customer")
                steps[1].release()

                steps[2].acquire()
                val customer = repliedCustomers.poll()
                logBarber("$id working on customer $customer")
                sleep(BARBER_WORK_SEC)
                logBarber("$id done on customer $customer")
                steps[3].release()

                steps[4].acquire()
                logBarber("$id has a rest")
                sleep(BARBER_REST_SEC)
            }
        }.apply{ start() }
    }

    fun newCustomer(): Thread{
        val id = cid.inc()
        return Thread{
            println("---Customer $id comes")
            if(freeSeats.dec() < 0){
                println("!!!!!!!!!!!!Customer $id has not seat and leaves")
                freeSeats.inc()
                return@Thread
            }
            println("---Customer $id sits and waits")
            steps[0].release()

            steps[1].acquire()
            println("---Customer $id responds, stands up and is served")
            freeSeats.inc()
            repliedCustomers.add(id)
            steps[2].release()

            steps[3].acquire()
            println("---Customer $id done and leaves")
            steps[4].release()
        }.apply{ start() }
    }

    private fun sleep(nSecond: Int){
        Thread.sleep(nSecond * 1000L)
    }
    private fun logBarber(msg: String){
        println("=".repeat(100)+"Barber "+msg)
    }

}

class SleepingBarbersTests{
    @Test
    fun testMultiThreadSleepingBarbers(){
        val seats = 3
        val barbersNum = 2
        val roundNum = 2
        val roundInterval = 30_000
        val roundCustomersNum = 15
        val customerInterval = 5000


        val sim = MultiThreadSleepingBarberSimulator(seats)
        val cth = LinkedList<Thread>()

        repeat(barbersNum) { sim.newBarber() }       // 2 barbers

        repeat(roundNum){
            repeat(roundCustomersNum){
                Thread.sleep(Random().nextInt(customerInterval).toLong())

                cth.add( sim.newCustomer() )
            }

            Thread.sleep(roundInterval.toLong())
        }

        cth.forEach { it.join() }
    }
}
