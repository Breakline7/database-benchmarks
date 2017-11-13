package partners.campaign

import partners.campaign.arangodb.ArangoDBBenchmark
import profig.{Config, ConfigApplication}

import scala.concurrent.Await
import scala.concurrent.duration._

object DatabaseBenchmarks extends ConfigApplication {
  private val databases: Map[String, DatabaseBenchmark] = Map(
    "arangodb" -> ArangoDBBenchmark
  )

  lazy val config = Config("database")

  override def main(args: Array[String]): Unit = start(args)

  override protected def run(): Unit = {
    config("benchmark").as[Option[String]] match {
      case Some(db) => databases.get(db) match {
        case Some(benchmark) => {
          Await.result(benchmark.run(), 10.minutes)
          scribe.info(s"Setup: ${benchmark.stats.setup}")
          scribe.info(s"Simple Inserts: ${benchmark.stats.simple.insert}")
          scribe.info(s"Simple Count: ${benchmark.stats.simple.count}")
          scribe.info(s"Cleanup: ${benchmark.stats.cleanup}")
          sys.exit()
        }
        case None => println(s"`$db` is not a valid database benchmark. Valid options are: ${databases.keySet.mkString(", ")}")
      }
      case None => println(s"The benchmark must be specified with the option --database.benchmark=<database>. Valid options are: ${databases.keySet.mkString(", ")}")
    }
  }
}