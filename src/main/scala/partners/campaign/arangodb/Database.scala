package partners.campaign.arangodb

import com.outr.arango.managed.{Graph, VertexCollection}

object Database extends Graph("benchmark") {
  val simple: VertexCollection[Simple] = vertex[Simple]("simple")
}
