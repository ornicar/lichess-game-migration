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

    val fromStr = args.lift(0).getOrElse("2010-07-25")
    val concurrency = args.lift(1).fold(4)(java.lang.Integer.parseInt)
    val maxDocs = Int.MaxValue

    val from = new DateTime(fromStr).withTimeAtStartOfDay()

    println(s"Migrate since $from with concurrency $concurrency")

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
          .documentSource(maxDocs = maxDocs)

        def readDoc(doc: BSONDocument): Encoded = (
          doc.getAsTry[String]("_id").get,
          doc.getAsTry[BSONBinary]("pg").get,
          doc.getAsTry[DateTime]("ca").get
        )

        val tickSource =
          Source.tick(Reporter.freq, Reporter.freq, None)

        def convert(g: Encoded): Future[Encoded] = Future {
          g.copy(
            _2 = try {
              BSONBinary(
                org.lichess.compression.game.Encoder.encode {
                  chess.format.pgn.Binary.readMoves(g._2.byteArray.toList).get.toArray
                },
                Subtype.GenericBinarySubtype
              )
            }
            catch {
              case e: java.lang.NullPointerException =>
                println(s"Error https://lichess.org/${g._1} ${g._3}")
                throw e
            }
          )
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
          .buffer(20000 min maxDocs, OverflowStrategy.backpressure)
          .map(d => Some(readDoc(d)))
          .merge(tickSource, eagerComplete = true)
          .via(Reporter)
          .mapAsyncUnordered(concurrency)(convert)
          .buffer(300 min maxDocs, OverflowStrategy.backpressure)
          .mapAsyncUnordered(concurrency * 3)(update)
          .runWith(Sink.ignore) andThen {
            case state =>
              close()
              system.terminate()
          }
    }
  }
}
