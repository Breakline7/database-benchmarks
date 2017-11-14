package partners.campaign

import java.io._

import io.youi.Unique

class SimpleDataInstance(total: Int, offset: Int) {
  private lazy val directory = new File("data")
  private lazy val file = new File(directory, s"simple$total.csv")
  private lazy val reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))

  lazy val all: List[SimpleData] = (0 until total).map(_ => next()).toList.flatten

  def next(): Option[SimpleData] = Option(reader.readLine()).map(_.split(',')).map(a => new SimpleData(a(0).toInt, a(1)))

  def generate(): Unit = if (!file.exists()) {
    scribe.info(s"Generating SimpleData for $total records at ${file.getName}...")
    directory.mkdirs()

    val b = new FileWriter(file)
    (0 until total).foreach { index =>
      val i = index + offset
      b.write(s"$i,${Unique(8)}\n")
    }
    b.flush()
    b.close()
    scribe.info("Data generation complete")
  }
}

object SimpleData {
  lazy val single: SimpleDataInstance = new SimpleDataInstance(10000, 0)
  lazy val bulk: SimpleDataInstance = new SimpleDataInstance(100000, 10000)

  def init(): Unit = {
    single.generate()
    bulk.generate()
  }
}

class SimpleData(val value: Int, val id: String)