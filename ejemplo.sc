import kantan.csv.ops.toCsvInputOps
import kantan.csv.rfc
import kantan.csv.generic._
import play.api.libs.json.Json
import java.io.File
import scala.collection.immutable.ListMap

val rawData = "C:/Users/Byron/Desktop/UTPL 9/FUNDAMENTOS DE BASE DE DATOS/B2/informe avance proyecto integrador (BASE DE DATOS)/movie_dataset.csv"
case class Movies(
                   index: String,
                   budget: String,
                   genres: String,
                   homepage: String,
                   id: String,
                   keywords: String,
                   original_language: String,
                   original_title: String,
                   overview: String,
                   popularity: Double,
                   production_companies: String,
                   production_countries: String,
                   release_date: String,
                   revenue: String,
                   runtime: Option[Double],
                   spoken_languages: String,
                   status: String,
                   tagline: String,
                   title: String,
                   vote_average: Double,
                   vote_count: String,
                   cast: String,
                   crew: String,
                   director: String)

val dataSource = new File(rawData).readCsv[List, Movies](rfc.withHeader(true).withCellSeparator(','))

val values = dataSource.collect({ case Right(movies) => movies })
val valuesL = dataSource.collect({ case Left(movies) => movies })

//values.foreach(println _)

//Cuantos datos Right y Left obtienen
values.size
valuesL.size
dataSource.size

//Tiempo promedio
val datasize = values.size
val runtimeAvg = values.map(_.runtime.getOrElse(0.0)).sum / datasize

//Agrupar por director
ListMap(values.groupBy(_.director)
  .map({case(k, v) => (k, v.size)})
  .toSeq.sortWith(_._2 >_._2):_*).foreach(println)

//Popularity
values.map(_.popularity).foreach(println)

//JSON
val prodCompList = values
  .map(mv => Json.parse(mv.production_companies))
  .flatMap(json => (json \\ "name"))
  .map(_.as[String])
  .distinct
  .sorted
prodCompList.foreach(println)








