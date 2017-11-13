package partners.campaign.arangodb

import com.outr.arango.DocumentOption
import partners.campaign.DatabaseBenchmark
import com.outr.arango._
import com.outr.arango.managed._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object ArangoDBBenchmark extends DatabaseBenchmark {
  override def setup(): Future[Unit] = Database.init().map(_ => ())

  override def insertSimple(key: String, value: Int): Future[Unit] = {
    Database.simple.insert(Simple(value, Some(key))).map(_ => ())
  }

  override def countSimple(): Future[Int] = {
    import Database._
    val query =
      aql"""
           FOR s IN $simple
           COLLECT WITH COUNT INTO length
           RETURN length
         """
    call[Int](query)
  }

  override def cleanup(): Future[Unit] = Database.delete().map(_ => ())
}

case class Simple(value: Int,
                  _key: Option[String],
                  _id: Option[String] = None,
                  _rev: Option[String] = None) extends DocumentOption

object Database extends Graph("benchmark") {
  val simple: VertexCollection[Simple] = vertex[Simple]("simple")
}