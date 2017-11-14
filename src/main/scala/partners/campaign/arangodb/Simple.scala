package partners.campaign.arangodb

import com.outr.arango.DocumentOption

case class Simple(value: Int,
                  _key: Option[String],
                  _id: Option[String] = None,
                  _rev: Option[String] = None) extends DocumentOption
