package partners.campaign

import java.io._

import io.youi.Unique

object SimpleData {
  private lazy val directory = new File("data")
  private lazy val file = new File(directory, "simple.csv")
  private lazy val reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))

  def next(): Option[SimpleData] = Option(reader.readLine()).map(_.split(',')).map(a => new SimpleData(a(0).toInt, a(1)))

  def generate(): Unit = {
    directory.mkdirs()

    val b = new FileWriter(file)
    (0 until 10000).foreach { index =>
      b.write(s"$index,${Unique(8)}\n")
    }
    b.flush()
    b.close()
  }
}

class SimpleData(val value: Int, val id: String)