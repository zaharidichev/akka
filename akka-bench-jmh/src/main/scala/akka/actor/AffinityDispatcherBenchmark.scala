package akka.actor

import java.util.concurrent.TimeUnit

import akka.actor.AffinityDispatcherBenchmark.Request

import scala.collection.mutable
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations

import scala.concurrent.Await
import org.openjdk.jmh.annotations._

/**
 * Created by zahari on 5/22/17.
 */

@State(Scope.Benchmark)
@annotations.BenchmarkMode(Array(Mode.Throughput))
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS, batchSize = 1)
@Measurement(iterations = 20)
class AffinityDispatcherBenchmark {
  import AffinityDispatcherBenchmark._

  implicit var system: ActorSystem = null

  var sender: ActorRef = null
  var receiver: ActorRef = null

  @Setup(Level.Trial)
  def setup(): Unit = {
    system = ActorSystem("ForkJoinActorBenchmark", ConfigFactory.parseString(
      s"""| akka {
          |   log-dead-letters = off
          |   actor {
          |     default-dispatcher {
          |       executor = "thread-pool-executor"
          |       thread-pool-executor {
          |         fixed-pool-size = 8
          |       }
          |       throughput = 5
          |     }
          |     thread-factory = with-affinity
          |
          |   }
          | }
      """.stripMargin
    ))
  }

  @TearDown(Level.Trial)
  def shutdown(): Unit = {
    system.terminate()
    Await.ready(system.whenTerminated, 30.seconds)
  }

  @Setup(Level.Invocation)
  def setupActors(): Unit = {
    sender = system.actorOf(Props[AffinityDispatcherBenchmark.SenderActor], "sender")
    receiver = system.actorOf(Props[AffinityDispatcherBenchmark.ReceiverActor], "receiver")
  }

  @TearDown(Level.Invocation)
  def tearDownActors(): Unit = {
    system.stop(sender)
    system.stop(receiver)
    val p = TestProbe()
    p.watch(sender)
    p.expectTerminated(sender, timeout)
    p.watch(receiver)
    p.expectTerminated(receiver, timeout)
  }

  @Benchmark
  @Measurement(timeUnit = TimeUnit.MILLISECONDS)
  @OperationsPerInvocation(messages)
  def pingPong(): Unit = {
    // only one message in flight

    sender.tell(Request, receiver)
    val p = TestProbe()
    p.watch(sender)
    p.expectTerminated(sender, timeout)
    p.watch(receiver)
    p.expectTerminated(receiver, timeout)
  }

}

object AffinityDispatcherBenchmark {

  case class Delivery(content: Int)
  case object Request
  final val timeout = 45.seconds
  final val messages = 30000

  class SenderActor() extends Actor {
    val storage = new mutable.Stack[Int]()
    storage.pushAll(0 until messages)
    override def receive = {
      case Request =>
        if (storage.isEmpty) {
          println("Sender stopping")
          context stop self
        } else {
          val payload = storage.pop()
          sender() ! Delivery(payload)
        }

    }
  }

  class ReceiverActor extends Actor {
    val storage = new mutable.Stack[Int]()
    override def receive = {
      case Delivery(payload) =>
        val lastValue = storage.lastOption.getOrElse(-1)
        storage.push(lastValue * payload)
        sender() ! Request
        if (storage.size == messages) {
          println("Receiver stopping")
          context stop self
        }
    }
  }
}
