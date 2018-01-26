package lichess

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import reactivemongo.api._
import reactivemongo.bson._

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import reactivemongo.akkastream.{ State, cursorProducer }

import DB.BSONDateTimeHandler
import org.joda.time.DateTime

object Main extends App {

  type Encoded = (String, BSONBinary, DateTime)
  type Decoded = (String, List[String], DateTime)

  override def main(args: Array[String]) {

    val fromStr = args.lift(0).getOrElse("2010-01-01")

    val from = new DateTime(fromStr).withDayOfMonth(1).withTimeAtStartOfDay()

    println(s"Migrate since $from")

    implicit val system = ActorSystem()
    val decider: Supervision.Decider = {
      case e: Exception =>
        println(e)
        Supervision.Resume
      case _ => Supervision.Stop
    }
    implicit val materializer = ActorMaterializer(
      ActorMaterializerSettings(system)
        .withInputBuffer(
          initialSize = 32,
          maxSize = 32
        ).withSupervisionStrategy(decider)
    )

    DB.get foreach {
      case (coll, close) =>

        val query = BSONDocument(
          "ca" -> BSONDocument("$gte" -> from),
          "v" -> BSONDocument("$exists" -> false), // no variants
          "if" -> BSONDocument("$exists" -> false), // no initial fen
          "pg" -> BSONDocument("$exists" -> true), // old PGN
          "hp" -> BSONDocument("$exists" -> false) // no new PGN
        )
        val projection = BSONDocument("pg" -> true, "ca" -> true)

        val gameSource = coll
          .find(query, projection)
          .sort(BSONDocument("ca" -> 1))
          .cursor[BSONDocument](readPreference = ReadPreference.secondaryPreferred)
          .documentSource(maxDocs = Int.MaxValue)

        def readDoc(doc: BSONDocument): Encoded = (
          doc.getAsTry[String]("_id").get,
          doc.getAsTry[BSONBinary]("pg").get,
          doc.getAsTry[DateTime]("ca").get
        )

        val tickSource =
          Source.tick(Reporter.freq, Reporter.freq, None)

        def decodeOld(g: Encoded): Future[Decoded] = Future {
          (g._1, chess.format.pgn.Binary.readMoves(g._2.byteArray.toList).get, g._3)
        }

        def encodeNew(g: Decoded): Future[Encoded] = Future {
          (g._1, BSONBinary(
            org.lichess.compression.game.Encoder.encode(g._2.toArray),
            Subtype.GenericBinarySubtype
          ), g._3)
        }

        def update(g: Encoded): Future[Unit] =
          coll.update(
            BSONDocument("_id" -> g._1),
            BSONDocument(
              "$set" -> BSONDocument("hp" -> g._2),
              "$unset" -> BSONDocument(
                "pg" -> true,
                "ps" -> true,
                "cl" -> true,
                "ph" -> true,
                "ur" -> true
              )
            )
          ).map(_ => ())

        gameSource
          .buffer(10000, OverflowStrategy.backpressure)
          .map(d => Some(readDoc(d)))
          .merge(tickSource, eagerComplete = true)
          .via(Reporter)
          .mapAsyncUnordered(8)(decodeOld)
          .mapAsyncUnordered(8)(encodeNew)
          .mapAsyncUnordered(8)(update)
          .runWith(Sink.ignore) andThen {
            case state =>
              close()
              system.terminate()
          }
    }
  }
}
