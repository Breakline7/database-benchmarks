package partners.campaign.mongodb

import org.mongodb.scala.MongoClient
import org.mongodb.scala.bson.collection.immutable.Document
import partners.campaign.{BenchmarkPaginated, DatabaseBenchmark, SimpleData}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import org.mongodb.scala.model.Filters._

object MongoDBBenchmark extends DatabaseBenchmark {
  private lazy val client = MongoClient()
  private lazy val db = client.getDatabase("benchmark")
  private lazy val simple = db.getCollection("simple")

  override def name: String = "mongodb"

  override def setup(): Future[Unit] = {
    cleanup().flatMap { _ =>
      db.createCollection("simple").toFuture().map(_ => ())
    }
  }

  override def insertSimple(data: SimpleData): Future[Unit] = {
    val doc = Document("_id" -> data.id, "value" -> data.value)
    simple.insertOne(doc).toFuture().map(_ => ())
  }

  override def insertSimpleBatch(entries: List[SimpleData]): Future[Unit] = {
    val docs = entries.map { data =>
      Document("_id" -> data.id, "value" -> data.value)
    }
    simple.insertMany(docs).toFuture().map(_ => ())
  }

  override def countSimple(): Future[Int] = {
    simple.count().toFuture().map(_.toInt)
  }

  override def selectSimpleRange(start: Int, end: Int): Future[List[String]] = {
    simple.find(and(gte("value", start), lt("value", end))).toFuture().map { docs =>
      docs.map(_.getString("_id")).toList
    }
  }

  override def sumSimple(): Future[Long] = {
    simple.find().foldLeft(0L)((total, document) => document.getInteger("value", 0) + total).toFuture()
  }

  override def paginatedSimple(batchSize: Int): BenchmarkPaginated = new BenchmarkPaginated {
    private var offset: Int = 0

    override def next(): Future[List[Int]] = try {
      simple.find().skip(offset).limit(batchSize).toFuture().map(_.map(_.getInteger("value", 0)).toList)
    } finally {
      offset += batchSize
    }
  }

  override def cleanup(): Future[Unit] = {
    db.drop().toFuture().map(_ => ())
  }
}
