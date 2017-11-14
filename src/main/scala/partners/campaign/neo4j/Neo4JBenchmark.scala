package partners.campaign.neo4j

import org.neo4j.driver.v1.{AuthTokens, GraphDatabase, Transaction, TransactionWork}
import partners.campaign.{BenchmarkPaginated, DatabaseBenchmark, SimpleData}

import scala.concurrent.Future

import scala.collection.JavaConverters._

object Neo4JBenchmark extends DatabaseBenchmark {
  private lazy val driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.none)
  private lazy val session = driver.session()

  override def name: String = "neo4j"

  override def setup(): Future[Unit] = Future {
    session
  }

  override def insertSimple(data: SimpleData): Future[Unit] = Future {
    session.writeTransaction(new TransactionWork[Unit] {
      override def execute(tx: Transaction): Unit = {
        val result = tx.run(
          """
            |CREATE (s: Simple)
            |SET s.value = $value
            |SET s.dataId = $id
            |RETURN id(a)
          """.stripMargin, Map[String, AnyRef]("value" -> data.value, "id" -> data.id).asJava)
        result.single().get(0).asString()
      }
    })
  }

  override def insertSimpleBatch(entries: List[SimpleData]): Future[Unit] = {
    Future.sequence(entries.map(insertSimple)).map(_ => ())
  }

  override def countSimple(): Future[Int] = Future {
    session.readTransaction(new TransactionWork[Int] {
      override def execute(tx: Transaction): Int = {
        val result = tx.run(
          """
            |start n=node(*)
            |match n
            |return count(n)
          """.stripMargin)
        result.single().get(0).asInt()
      }
    })
  }

  override def selectSimpleRange(start: Int, end: Int): Future[List[String]] = Future {
    session.readTransaction(new TransactionWork[List[String]] {
      override def execute(tx: Transaction): List[String] = {
        val result = tx.run(
          """
            |MATCH (s: Simple)
            |WHERE s.value >= $start AND s.value < $end
            |RETURN s.dataId
          """.stripMargin,
          Map[String, AnyRef]("start" -> start, "end" -> end).asJava
        )
        result.list().asScala.toList.map(_.values().get(0).asString())
      }
    })
  }

  override def sumSimple(): Future[Long] = Future {
    session.readTransaction(new TransactionWork[Long] {
      override def execute(tx: Transaction): Long = {
        val result = tx.run(
          """
            |MATCH (s: Simple)
            |RETURN sum(s.value)
          """.stripMargin
        )
        result.single().get(0).asLong()
      }
    })
  }

  override def paginatedSimple(batchSize: Int): BenchmarkPaginated = {
    ???
  }

  override def cleanup(): Future[Unit] = Future {
    session.close()
    driver.close()
  }
}
