package lichess

import com.typesafe.config.ConfigFactory
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import reactivemongo.api._
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson._

import org.joda.time._

object DB {

  private val config = ConfigFactory.load()
  private val dbUri = config.getString("db.uri")

  val dbName = "lichess"
  val collName = "game5"

  val driver = new reactivemongo.api.MongoDriver
  val conn = driver connection MongoConnection.parseURI(dbUri).get

  def get: Future[(BSONCollection, () => Unit)] =
    conn.database(dbName).map { db =>
      (
        db collection "game5",
        (() => {
          conn.close()
          driver.close()
        })
      )
    }

  implicit object BSONDateTimeHandler extends BSONHandler[BSONDateTime, DateTime] {
    def read(time: BSONDateTime) = new DateTime(time.value, DateTimeZone.UTC)
    def write(jdtime: DateTime) = BSONDateTime(jdtime.getMillis)
  }
}
