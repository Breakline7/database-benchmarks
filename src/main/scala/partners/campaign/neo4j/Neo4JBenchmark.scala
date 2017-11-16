package partners.campaign.neo4j

import org.neo4j.driver.v1._
import partners.campaign.{BenchmarkPaginated, DatabaseBenchmark, SimpleData}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._

object Neo4JBenchmark extends DatabaseBenchmark {
  private lazy val driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.none)

  override def name: String = "neo4j"

  override def setup(): Future[Unit] = Future {
    deleteAll()
  }
  private def withSession[R](f: Session => R): R = {
    val session = driver.session()
    try {
      f(session)
    } finally {
      session.close()
    }
  }

  override def insertSimple(data: SimpleData): Future[Unit] = withSession { session =>
    try {
      val result = session.run(
        """
          |CREATE (s: Simple)
          |SET s.value = $value
          |SET s.dataId = $id
          |RETURN id(s)
        """.stripMargin, Map[String, AnyRef]("value" -> new Integer(data.value), "id" -> data.id).asJava
      )
      result.single().get(0)
      Future.successful(())
    } catch {
      case t: Throwable => throw new RuntimeException(s"Insert failed for $data", t)
    }
  }

  override def insertSimpleBatch(entries: List[SimpleData]): Future[Unit] = withSession { session =>
    Future.sequence(entries.map(insertSimple)).map(_ => ())
  }

  override def countSimple(): Future[Int] = Future {
    withSession { session =>
      val result = session.run(
        """
        |START n=node(*)
        |MATCH (n: Simple)
        |RETURN count(n)
      """.
          stripMargin)
    result.single().get(0).asInt()
      }
  }

  override def selectSimpleRange(start: Int, end: Int): Future[List[String]] = Future {
    withSession { session =>
      val result = session.run(
        """
        |MATCH (s: Simple)
        |WHERE s.value >= $start AND s.value < $end
        |RETURN s.dataId
      """.
          stripMargin,
      Map[String, AnyRef]("start" -> new Integer(start), "end" -> new Integer(end)).asJava
    )
    result.list().asScala.toList.map(_.values().get(0).asString())
      }
  }

  override def sumSimple(): Future[Long] = Future {
    withSession { session =>
      val result = session.run(
        """
        |MATCH (s: Simple)
        |RETURN SUM(s.value)
      """.stripMargin
    )
      result
        .single().get(0).asLong()
      }
  }

  override def paginatedSimple(batchSize: Int): BenchmarkPaginated = new BenchmarkPaginated {
    private var offset: Int = 0

    override def next(): Future[List[Int]] = Future {
      try {
        withSession { session =>
          val result = session.run(
            """
            |MATCH (s: Simple)
            |RETURN s.value
            |ORDER BY s.value
            |SKIP $offset
            |LIMIT $limit
          """.
              stripMargin,
          Map[String, AnyRef]("offset" -> new Integer(offset), "limit" -> new Integer(batchSize)).asJava
        )
        result.list().asScala.toList.map(_.values().get(0).asInt())
          }
      } finally {
        offset += batchSize
      }
    }
  }

  private def deleteAll(): Unit = withSession { session =>
    session.run(
      """
        |MATCH (s: Simple)
        |DELETE s
      """.stripMargin)
  }

  override def cleanup(): Future[Unit] = Future {
    deleteAll()
    driver.close()
  }
}
