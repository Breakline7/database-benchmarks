package partners.campaign.arangodb

import com.outr.arango.DocumentOption
import partners.campaign.{BenchmarkPaginated, DatabaseBenchmark, SimpleData}
import com.outr.arango._
import com.outr.arango.managed._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import io.circe.generic.auto._

object ArangoDBBenchmark extends DatabaseBenchmark {
  override def name: String = "arangodb"

  override def setup(): Future[Unit] = Database.init().map(_ => ())

  override def insertSimple(data: SimpleData): Future[Unit] = {
    Database.simple.insert(Simple(data.value, Some(data.id))).map(_ => ())
  }

  override def insertSimpleBatch(entries: List[SimpleData]): Future[Unit] = {
    Database.simple.collection.document.bulk.insert(entries, waitForSync = true).map(_ => ())
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

  override def selectSimpleRange(start: Int, end: Int): Future[List[String]] = {
    import Database._
    val query =
      aql"""
           FOR s IN $simple
           FILTER s.value >= $start
           FILTER s.value <= $end
           RETURN s._key
         """
    cursor[String](query, batchSize = Some(end - start)).map(_.result)
  }

  override def sumSimple(): Future[Long] = {
    import Database._
    val query =
      aql"""
           FOR s IN $simple
           COLLECT AGGREGATE total = SUM(s.value)
           RETURN total
         """
    call[Long](query)
  }

  override def paginatedSimple(batchSize: Int): BenchmarkPaginated = {
    val results = Database.simple.all(batchSize).map(_.toIterator)
    new BenchmarkPaginated {
      override def next(): Future[List[Int]] = results.map(_.take(batchSize).map(_.value).toList)
    }
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