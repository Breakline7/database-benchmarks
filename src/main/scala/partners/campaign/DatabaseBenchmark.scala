package partners.campaign

import java.io.File
import java.lang.management.ManagementFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scribe.formatter.FormatterBuilder
import scribe.writer.FileWriter
import scribe.{LogHandler, Logger}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait DatabaseBenchmark {
  private val threads = 10
  private val map = new ConcurrentHashMap[String, Long]

  private object loggers {
    val general: Logger = createLogger("general", Some(Logger.rootName))
    val simple: Logger = createLogger("simple")
  }
  private val counter = new AtomicLong(0L)

  def name: String

  protected def started(key: String): Unit = map.put(key, System.nanoTime())
  protected def finished(key: String): Unit = map.put(key, System.nanoTime() - map.get(key))

  private def createLogger(loggerName: String, parentName: Option[String] = None): Logger = {
    val l = Logger(parentName)
    l.clearHandlers()
    l.addHandler(LogHandler(
      formatter = FormatterBuilder().date().string(" ").message.newLine,
      writer = new FileWriter(new File("logs"), () => s"$name.$loggerName.log", append = false)
    ))
    l
  }

  private def monitorInfo: String = {
    val os = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[com.sun.management.OperatingSystemMXBean]
    List(
      "Inserted" -> counter.get(),
      "CommittedVirtualMemory" -> os.getCommittedVirtualMemorySize,
      "FreePhysicalMemory" -> os.getFreePhysicalMemorySize,
      "FreeSwapSpace" -> os.getFreeSwapSpaceSize,
      "ProcessCpuLoad" -> os.getProcessCpuLoad,
      "ProcessCpuTime" -> os.getProcessCpuTime,
      "SystemCpuLoad" -> os.getSystemCpuLoad,
      "SystemLoadAverage" -> os.getSystemLoadAverage,
      "TotalPhysicalMemory" -> os.getTotalPhysicalMemorySize,
      "TotalSwapSpace" -> os.getTotalSwapSpaceSize
    ).map(t => s"${t._1}=${t._2}").mkString(", ")
  }

  final def run(): Future[Unit] = {
    var running = true
    val monitor = new Thread {
      override def run(): Unit = {
        while (running) {
          loggers.simple.info(monitorInfo)
          Thread.sleep(1000L)
        }
        loggers.simple.info(monitorInfo)
      }
    }

    scribe.info(s"Running benchmark ${getClass.getSimpleName}...")
    scribe.info("Running setup...")
    started("setup")
    val future = setup().flatMap { _ =>
      finished("setup")

      scribe.info("Inserting simple records...")
      started("simple.insert")
      monitor.start()
      Future.sequence((0 until threads).toList.map(_ => simpleInsertThread()))
    }.flatMap { _ =>
      running = false
      finished("simple.insert")

      scribe.info("Count simple records...")
      started("simple.count")
      countSimple()
    }.flatMap { count =>
      finished("simple.count")
      loggers.general.info(s"Simple Records: $count")

      val large = SimpleData.bulk.all
      started("simple.bulkInsert")
      insertSimpleBatch(large)
    }.flatMap { _ =>
      finished("simple.bulkInsert")
      countSimple()
    }.flatMap { count =>
      loggers.general.info(s"Simple Records: $count")

      started("simple.select100")
      selectSimpleRange(900, 1000)
    }.flatMap { simple100 =>
      finished("simple.select100")
      loggers.general.info(s"Simple 100: ${simple100.size}")

      started("simple.sum")
      sumSimple()
    }.flatMap { sum =>
      finished("simple.sum")
      loggers.general.info(s"Simple Sum: $sum")

      started("simple.pagination")
      recursivePagination(paginatedSimple(100))
    }.flatMap { paginationSum =>
      finished("simple.pagination")
      loggers.general.info(s"Simple Pagination Sum: $paginationSum")

      scribe.info("Cleanup...")
      started("cleanup")
      cleanup()
    }.map { _ =>
      finished("cleanup")

      val l = loggers.general
      l.info(s"Setup: ${stats.setup}")
      l.info(s"Simple Inserts: ${stats.simple.insert}")
      l.info(s"Simple Count: ${stats.simple.count}")
      l.info(s"Simple Bulk Inserts: ${stats.simple.bulkInsert}")
      l.info(s"Simple Select 100: ${stats.simple.select100}")
      l.info(s"Simple Sum: ${stats.simple.sum}")
      l.info(s"Simple Pagination: ${stats.simple.pagination}")
      l.info(s"Cleanup: ${stats.cleanup}")
    }
    future.failed.foreach(scribe.error(_))
    future
  }

  def setup(): Future[Unit]

  def insertSimple(data: SimpleData): Future[Unit]

  def insertSimpleBatch(entries: List[SimpleData]): Future[Unit]

  def countSimple(): Future[Int]

  def selectSimpleRange(start: Int, end: Int): Future[List[String]]

  def sumSimple(): Future[Long]

  def paginatedSimple(batchSize: Int): BenchmarkPaginated

  def cleanup(): Future[Unit]

  object stats {
    private def get(key: String): Double = map.get(key) / 1000000000.0

    def setup: Double = get("setup")
    object simple {
      def insert: Double = get("simple.insert")
      def bulkInsert: Double = get("simple.bulkInsert")
      def count: Double = get("simple.count")
      def select100: Double = get("simple.select100")
      def sum: Double = get("simple.sum")
      def pagination: Double = get("simple.pagination")
    }
    def cleanup: Double = get("cleanup")
  }

  private def simpleInsertThread(): Future[Unit] = SimpleData.single.next() match {
    case Some(data) => insertSimple(data).flatMap { _ =>
      counter.incrementAndGet()

      simpleInsertThread()
    }
    case None => Future.successful(())
  }

  private def recursivePagination(paginated: BenchmarkPaginated, sum: Long = 0L): Future[Long] = paginated.next().flatMap { values =>
    if (values.isEmpty) {
      Future.successful(sum)
    } else {
      recursivePagination(paginated, sum + values.sum)
    }
  }
}

trait BenchmarkPaginated {
  def next(): Future[List[Int]]
}