/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.cluster.ddata

import java.io.File
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.breakOut
import scala.concurrent.duration._
import scala.util.control.NonFatal

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.DeadLetterSuppression
import akka.actor.ExtendedActorSystem
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator.Internal.DataEnvelope
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest
import com.typesafe.config.Config
import org.mapdb.DBMaker
import org.mapdb.DataIO
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.HTreeMap
import org.mapdb.Serializer

/**
 * An actor implementing the durable store for the Distributed Data `Replicator`
 * have to implement the protocol with the messages defined here.
 *
 * At startup the `Replicator` creates the durable store actor and sends the
 * `Load` message to it. It must then reply with 0 or more `LoadData` messages
 * followed by one `LoadCompleted` message to the `sender` (the `Replicator`).
 *
 * When the `Replicator` needs to store a value it sends a `Store` message
 * to the durable store actor, which must then reply with the `successMsg` or
 * `failureMsg` to the `replyTo`.
 */
object DurableStore {
  final case class Store(key: String, data: ReplicatedData, reply: Option[StoreReply])
  final case class StoreReply(successMsg: Any, failureMsg: Any, replyTo: ActorRef)

  case object Load
  final case class LoadData(data: Map[String, ReplicatedData])
  case object LoadCompleted
}

object MapDbDurableStore {
  def props(config: Config): Props =
    Props(new MapDbDurableStore(config))

  private case object Commit extends DeadLetterSuppression
}

final class MapDbDurableStore(config: Config) extends Actor with ActorLogging {
  import DurableStore._
  import MapDbDurableStore.Commit

  val serializer = SerializationExtension(context.system).serializerFor(classOf[DataEnvelope])
  val commitInterval = config.getString("mapdb.commit-interval").toLowerCase match {
    case "off" ⇒ Duration.Zero
    case _     ⇒ config.getDuration("mapdb.commit-interval", MILLISECONDS).millis
  }

  val db = {
    val file = config.getString("mapdb.file") match {
      case path if path.endsWith(".ddata") ⇒
        new File(path)
      case dir ⇒
        new File(dir, s"${context.system.name}-${self.path.parent.name}-${Cluster(context.system).selfAddress.port.get}.ddata")
    }
    file.getParentFile.mkdirs()

    val builder = DBMaker.fileDB(file)
      .fileMmapEnableIfSupported()
      .fileMmapPreclearDisable()
      .allocateStartSize(config.getBytes("mapdb.allocate-start-size"))
      .allocateIncrement(config.getBytes("mapdb.allocate-increment"))
      .concurrencyDisable() // only access from this actor
      .closeOnJvmShutdown()

    if (config.getBoolean("mapdb.write-ahead-log"))
      builder.transactionEnable() // WAL for crash safety

    builder.make()
  }
  val store: HTreeMap[String, ReplicatedData] =
    db.hashMap("store")
      .keySerializer(Serializer.STRING)
      .valueSerializer(new MapdbReplicatedDataSerializer(context.system))
      .createOrOpen()

  val uncommitted = new java.util.HashMap[String, ReplicatedData]

  override def postStop(): Unit = {
    super.postStop()
    commit()
    db.close()
  }

  def receive = init

  def init: Receive = {
    case Load ⇒
      try {
        val loadData = LoadData(store.entrySet.asScala.map { entry ⇒
          entry.getKey → entry.getValue
        }(breakOut))
        if (loadData.data.nonEmpty)
          sender() ! loadData
        sender() ! LoadCompleted
      } catch {
        case NonFatal(e) ⇒
          log.error(e, "failed to load data")
          sender() ! LoadCompleted
      } finally
        context.become(active)
  }

  def active: Receive = {
    case Store(key, data, reply) ⇒
      try {
        if (commitInterval.length == 0) {
          // optimize calls from put, no need to deserialize old value, using ThreadLocal
          MapdbReplicatedDataSerializer.callFromPut.set(MapdbReplicatedDataSerializer.NotUsedOldValue)
          try store.put(key, data) finally
            MapdbReplicatedDataSerializer.callFromPut.set(null)
          db.commit()
        } else {
          if (uncommitted.isEmpty)
            context.system.scheduler.scheduleOnce(commitInterval, self, Commit)(context.system.dispatcher)
          uncommitted.put(key, data)
        }
        reply match {
          case Some(StoreReply(successMsg, _, replyTo)) ⇒
            replyTo ! successMsg
          case None ⇒
        }
      } catch {
        case NonFatal(e) ⇒
          log.error(e, "failed to store [{}]", key)
          reply match {
            case Some(StoreReply(_, failureMsg, replyTo)) ⇒
              replyTo ! failureMsg
            case None ⇒
          }
      }

    case Commit ⇒
      commit()
  }

  def commit(): Unit = {
    if (!uncommitted.isEmpty()) {
      val t0 = System.nanoTime()
      try {
        // optimize calls from put, no need to deserialize old value, using ThreadLocal
        MapdbReplicatedDataSerializer.callFromPut.set(MapdbReplicatedDataSerializer.NotUsedOldValue)
        try store.putAll(uncommitted) finally
          MapdbReplicatedDataSerializer.callFromPut.set(null)
        db.commit()
        if (log.isDebugEnabled)
          log.debug("store and commit of [{}] entries took [{} ms]", uncommitted.size,
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime - t0))
      } catch {
        case NonFatal(e) ⇒
          import scala.collection.JavaConverters._
          log.error(e, "failed to store [{}]", uncommitted.keySet.asScala.mkString(","))
      } finally {
        uncommitted.clear()
      }
    }
  }

}

/**
 * INTERNAL API
 */
private[akka] object MapdbReplicatedDataSerializer {
  val callFromPut = new ThreadLocal[NotUsedOldValue.type]

  object NotUsedOldValue extends ReplicatedData {
    override type T = NotUsedOldValue.type
    override def merge(that: NotUsedOldValue.type): NotUsedOldValue.type = this
  }

}

/**
 * INTERNAL API
 */
private[akka] class MapdbReplicatedDataSerializer(system: ActorSystem) extends Serializer[ReplicatedData] {

  val serialization = SerializationExtension(system)
  val transportInformation: Serialization.Information = {
    val address = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    Serialization.Information(address, system)
  }

  override def serialize(out: DataOutput2, value: ReplicatedData): Unit = {
    // Serialize actor references with full address information (defaultAddress).
    Serialization.currentTransportInformation.withValue(transportInformation) {
      val serializer = serialization.findSerializerFor(value)
      val manifest = serializer match {
        case ser2: SerializerWithStringManifest ⇒ ser2.manifest(value)
        case _ ⇒
          if (serializer.includeManifest) value.getClass.getName else ""
      }
      val bytes = serializer.toBinary(value)

      DataIO.packInt(out, serializer.identifier)
      out.writeUTF(manifest)
      DataIO.packInt(out, bytes.length)
      out.write(bytes)
    }
  }

  override def deserialize(in: DataInput2, available: Int): ReplicatedData = {
    MapdbReplicatedDataSerializer.callFromPut.get match {
      case null ⇒
        val serializerId = DataIO.unpackInt(in)
        val manifest = in.readUTF()
        val size = DataIO.unpackInt(in)
        val bytes = new Array[Byte](size)
        in.readFully(bytes)
        serialization.deserialize(bytes, serializerId, manifest).get.asInstanceOf[ReplicatedData]
      case value ⇒
        // optimize calls from put, no need to deserialize old value, using ThreadLocal
        value
    }
  }

  override def isTrusted(): Boolean = {
    true
  }

}
