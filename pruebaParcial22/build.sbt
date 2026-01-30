ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.7"

val circeVersion = "0.14.10"

lazy val root = (project in file("."))
  .settings(
    name := "pruebaParcial2",
    libraryDependencies ++= Seq(
      // RxScala y ReScala
      "io.reactivex" % "rxscala_2.13" % "0.27.0",
      "de.tu-darmstadt.stg" %% "rescala" % "0.35.0",

      // FS2 + CSV
      "org.gnieh" %% "fs2-data-csv" % "1.11.1",
      "org.gnieh" %% "fs2-data-csv-generic" % "1.11.1",
      "co.fs2" %% "fs2-core" % "3.12.2",
      "co.fs2" %% "fs2-io" % "3.12.2",

      // Circe (JSON)
      "io.circe" %% "circe-core"    % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser"  % circeVersion,

      "org.tpolecat" %% "doobie-core" % "1.0.0-RC11",      // Dependencias de doobie
      "org.tpolecat" %% "doobie-hikari" % "1.0.0-RC11",    // Para gestión de conexiones
      "com.mysql" % "mysql-connector-j" % "9.1.0",       // Driver para MySQL
      "com.typesafe" % "config"           % "1.4.2",       // Para gestión de archivos de configuración
      "org.slf4j" % "slf4j-simple" % "2.0.16",

      "org.tpolecat" %% "doobie-core" % "1.0.0-RC11",      // Dependencias de doobie
      "org.tpolecat" %% "doobie-hikari" % "1.0.0-RC11",    // Para gestión de conexiones
      "com.mysql" % "mysql-connector-j" % "9.1.0",         // Driver para MySQL
      "com.oracle.database.jdbc" % "ojdbc8" % "21.3.0.0",  // Driver para Oracle
      "com.typesafe" % "config"           % "1.4.2",       // Para gestión de archivos de configuración
      "org.slf4j" % "slf4j-simple" % "2.0.16"              // Implementaciónd de loggin
    )
  )