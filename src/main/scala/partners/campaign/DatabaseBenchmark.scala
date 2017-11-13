package partners.campaign

import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait DatabaseBenchmark {
  private val threads = 10
  private val map = new ConcurrentHashMap[String, Long]

  protected def started(key: String): Unit = map.put(key, System.nanoTime())
  protected def finished(key: String): Unit = map.put(key, System.nanoTime() - map.get(key))

  final def run(): Future[Unit] = {
    scribe.info(s"Running benchmark ${getClass.getSimpleName}...")
    scribe.info("Running setup...")
    started("setup")
    val future = setup().flatMap { _ =>
      finished("setup")
      scribe.info("Inserting simple records...")
      started("simple.insert")

      Future.sequence((0 until threads).toList.map(_ => simpleInsertThread()))
    }.flatMap { _ =>
      finished("simple.insert")
      scribe.info("Count simple records...")
      started("simple.count")
      countSimple()
    }.flatMap { count =>
      finished("simple.count")
      scribe.info(s"Simple Records: $count")
      scribe.info("Cleanup...")
      started("cleanup")
      cleanup()
    }.map { _ =>
      finished("cleanup")
    }
    future.failed.foreach(scribe.error(_))
    future
  }

  def setup(): Future[Unit]

  def insertSimple(id: String, value: Int): Future[Unit]

  def countSimple(): Future[Int]

  def cleanup(): Future[Unit]

  object stats {
    private def get(key: String): Double = map.get(key) / 1000000000.0

    def setup: Double = get("setup")
    object simple {
      def insert: Double = get("simple.insert")
      def count: Double = get("simple.count")
    }
    def cleanup: Double = get("cleanup")
  }

  private def simpleInsertThread(): Future[Unit] = SimpleData.next() match {
    case Some(data) => insertSimple(data.id, data.value).flatMap(_ => simpleInsertThread())
    case None => Future.successful(())
  }
}