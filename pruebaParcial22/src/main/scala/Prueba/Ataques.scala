package Prueba

import cats.effect._
import doobie._
import doobie.implicits._
import fs2.text
import fs2.io.file.{Files, Path}
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*
import cats.implicits._

object Ataques extends IOApp.Simple{

  case class Ataque(
                     ID: Int,
                     Tipo_Ataque: String,
                     IP_Origen: String,
                     IP_Destino: String,
                     Severidad: String,
                     País_Origen: String,
                     País_Destino: String,
                     Fecha_Ataque: String,
                     Duracion_Minutos: Int,
                     Contenido_Sensible_Comprometido: Boolean
                   )

  given CsvRowDecoder[Ataque, String] =
    deriveCsvRowDecoder[Ataque]

  private val xa = Transactor.fromDriverManager[IO](
    driver = "com.mysql.cj.jdbc.Driver",
    url = "jdbc:mysql://localhost:3306/seguridad",
    user = "root",
    password = "Bryan2007XD.123",
    logHandler = None
  )

  // INSERT SQL
  private def insertAtaque(a: Ataque): ConnectionIO[Int] =
    sql"""
      INSERT INTO ataque_informatico (
        id, tipo_ataque, ip_origen, ip_destino, severidad,
        pais_origen, pais_destino, fecha_ataque,
        duracion_minutos, contenido_sensible_comprometido
      )
      VALUES (
        ${a.ID},
        ${a.Tipo_Ataque},
        ${a.IP_Origen},
        ${a.IP_Destino},
        ${a.Severidad},
        ${a.País_Origen},
        ${a.País_Destino},
        ${a.Fecha_Ataque},
        ${a.Duracion_Minutos},
        ${a.Contenido_Sensible_Comprometido}
      )
    """.update.run

  private def listAllAt(): ConnectionIO[List[
    (Int, String, String, String, String, String, String, String, Int, Boolean)
  ]] =
    sql"""
      SELECT * FROM ataque_informatico
    """.query[
      (Int, String, String, String, String, String, String, String, Int, Boolean)
    ].to[List]

  private val filePath =
    Path("src/main/resources/ataques.csv")

  private val leerCSV: IO[List[Ataque]] =
    Files[IO]
      .readAll(filePath)
      .through(text.utf8.decode)
      .through(decodeUsingHeaders[Ataque](','))
      .compile
      .toList

  override def run: IO[Unit] =
    for {
      ataques <- leerCSV
      _ <- IO.println(s"Registros leídos del CSV: ${ataques.length}")

      _ <- ataques.traverse { a =>
        insertAtaque(a).transact(xa)
      }

      _ <- IO.println("Datos insertados correctamente")

      lista <- listAllAt().transact(xa)

      _ <- IO.println("----- LISTADO DE ATAQUES -----")
      _ <- IO(
        lista.foreach {
          case (id, tipo, ipO, ipD, sev, pO, pD, fecha, dur, cont) =>
            println(s"$id | $tipo | $ipO -> $ipD | $sev | $fecha | $dur min | $cont")
        }
      )

    } yield ()
}
