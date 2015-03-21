package com.github.matthewpflueger.pipedrive

import java.nio.charset.Charset
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.http.Http
import akka.http.Http.OutgoingConnection
import akka.http.model._
import akka.http.model.headers.Location
import akka.stream.scaladsl._
import akka.stream.{OverflowStrategy, ActorFlowMaterializer, UniformFanInShape}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Try, Failure, Success}


object PipeDrive extends App {
  //command line arguments...
  val startAt = 0
  val max = 3
  val parallelize = 1
  val saveToDirectory = Paths.get("/Users/matthewpflueger/tmp/PipeDrive")
  val rootList = saveToDirectory.resolve("file_root_list.csv")
  val fileList = saveToDirectory.resolve("file_list.csv")
  val apiToken = "8accadbb8209e028305294910814c1a06205d4cc"
  val timeoutSeconds = 500

  Try(Files.createDirectory(saveToDirectory))
  if (!Files.isDirectory(saveToDirectory) || !Files.isWritable(saveToDirectory)) {
    Console.err.println(s"$saveToDirectory is not a directory or is not writable")
    sys.exit(1)
  }


  val conf = ConfigFactory.load()
  implicit val system = ActorSystem("test", conf)
  implicit val executionContext = system.dispatcher
  implicit val materializer = ActorFlowMaterializer()

  val timeout = FiniteDuration(timeoutSeconds, "seconds")
  val count = new AtomicInteger(0)


  def makeFileDownloadHttpRequest(fileId: Int) =
      HttpRequest(HttpMethods.GET, Uri(s"/v1/files/${fileId}/download?api_token=${apiToken}"))

  def makeFilesHttpRequest(startAt: Int) = {
    val req = HttpRequest(HttpMethods.GET, Uri(s"/v1/files?start=${startAt}&api_token=${apiToken}"))
    println(s"Made ${req}")
    req
  }

  case class DownloadedPipeDriveFile(
      pipeDriveFile: PipeDriveFile,
      res: HttpResponse)

  case class PipeDriveFile(
      req: HttpRequest,
      id: Int,
      count: Int,
      _name: String,
      _orgName: String,
      _dealName: String) {

    val orgName = Option(_orgName).map(_.replace(",", "")).getOrElse("null")
    val dealName = Option(_dealName).map(_.replace(",", "")).getOrElse("null")
    val name = _name.replace(",", "")
    val file = s"${orgName}_${dealName}_${name}"
  }

  case class PipeDriveFiles(
      files: List[PipeDriveFile],
      nextStart: Option[Int])


  val pipeDriveConnection: Flow[HttpRequest, HttpResponse, Future[OutgoingConnection]] =  Http().outgoingConnection("api.pipedrive.com").buffer(1000, OverflowStrategy.fail)
  val s3Connection: Flow[HttpRequest, HttpResponse, Future[OutgoingConnection]] =  Http().outgoingConnection("pipedrive-files.s3.amazonaws.com").buffer(1000, OverflowStrategy.fail)

  val rootListWriter = Files.newBufferedWriter(
    rootList,
    Charset.defaultCharset(),
    StandardOpenOption.CREATE,
    StandardOpenOption.TRUNCATE_EXISTING,
    StandardOpenOption.WRITE)

  def makePipeDriveFiles(str: String): PipeDriveFiles = {
    val json = upickle.json.read(str)

    val success = json("success").value.asInstanceOf[Boolean]
    println(s"Successful fetch of file list: $success")

    if (success) {
      val data = json("data")
      val hasMore = json("additional_data")("pagination")("more_items_in_collection").value.asInstanceOf[Boolean]
      val nextStart =
        if (hasMore) {
          Some(json("additional_data")("pagination")("next_start").value.asInstanceOf[Double].toInt)
        } else None
      val files = data.value.asInstanceOf[ArrayBuffer[upickle.Js.Value]].map { v =>
        val id = v("id").value.asInstanceOf[Double].toInt
        val name = v("name").value.asInstanceOf[String]
        val orgName = v("org_name").value.asInstanceOf[String]
        val dealName = v("deal_name").value.asInstanceOf[String]

        PipeDriveFile(
          makeFileDownloadHttpRequest(id),
          id,
          count.incrementAndGet(),
          name,
          orgName,
          dealName)
      }.map { pf =>
        rootListWriter.write(s"${pf.file}, ${pf.orgName}")
        rootListWriter.newLine()
        pf
      }

      PipeDriveFiles(files.toList, nextStart)

    } else throw new RuntimeException("Failed to fetch file list!") //PipeDriveFiles(List.empty[PipeDriveFile], None)
  }

  val fromStringToPipeDriveFiles: Flow[ByteString, PipeDriveFiles, Unit] =
      Flow[ByteString].map { bs => makePipeDriveFiles(bs.utf8String) }

  val fromHttpResponseToPipeDriveFilesTuple: Flow[HttpResponse, (List[PipeDriveFile], Option[Int]), Unit] =
      Flow[HttpResponse]
          // for ease of development we make the entity fully materialize
          .map(_.entity.toStrict(timeout).map(e => makePipeDriveFiles(e.data.utf8String)))
          // this works but since the entity is streaming we would need to buffer
          // bytes if the json parse fails due to not enough data
          //.map(_.entity.dataBytes.via(fromStringToPipeDriveFiles).runWith(Sink.head()))
          //.mapAsyncUnordered(_.map { pfs => (pfs.files, pfs.nextStart) })
          //.mapAsync(_.map { pfs => (pfs.files, pfs.nextStart) })
          .mapAsync(_.map { pfs => (pfs.files, pfs.nextStart) })
          .filter(tuple => tuple._1.nonEmpty)

  val concatPipeDriveFiles: Flow[List[PipeDriveFile], PipeDriveFile, Unit] =
      Flow[List[PipeDriveFile]].mapConcat(identity(_))

  val nextStartToHttpRequest: Flow[Option[Int], HttpRequest, Unit] =
      Flow[Option[Int]].filter(_.isDefined).map(_.map(makeFilesHttpRequest(_)).get)


  val maxToDownload = Flow[PipeDriveFile].filter(_.count <= max)

  val downloadGraph = FlowGraph.partial() { implicit b =>
    import akka.stream.scaladsl.FlowGraph.Implicits._

    val merge = b.add(MergePreferred[HttpRequest](1))
    val unzip = b.add(Unzip[List[PipeDriveFile], Option[Int]]())

    val bcast = b.add(Broadcast[PipeDriveFile](1)) //, OperationAttributes.inputBuffer(1024, 2048)))

    // we could provide a start and an end but putting it outside
    // the graph allows one to specify custom starts and ends...
    merge ~> pipeDriveConnection ~> fromHttpResponseToPipeDriveFilesTuple ~> unzip.in
    unzip.out0 ~> concatPipeDriveFiles ~> maxToDownload ~> bcast
    unzip.out1 ~> nextStartToHttpRequest ~> merge.preferred

    UniformFanInShape(bcast.out(0), merge.in(0))
  }


  val followRedirect = Flow[HttpResponse]
      .filter(_.status == StatusCodes.Found)
      .map(_.header[Location])
      .filter(_.isDefined)
      .map { l => println(s"Location ${l.get.uri}"); HttpRequest(HttpMethods.GET, l.get.uri) }

  def printResponse(res: HttpResponse): Unit = println(s"Response ${res.status}: ${res.headers.mkString(", ")}")

  val debugResponse = Flow[HttpResponse].map { res => printResponse(res); res }

  val downloadFileGraph = FlowGraph.partial() { implicit b =>
    import akka.stream.scaladsl.FlowGraph.Implicits._

    val balancer = b.add(Balance[PipeDriveFile](parallelize))
    val merge = b.add(Merge[DownloadedPipeDriveFile](parallelize))


    for (_ <- 1 to parallelize) {
      val bcast = b.add(Broadcast[PipeDriveFile](2))
      val zip = b.add(Zip[PipeDriveFile, HttpResponse]())

      balancer ~> bcast ~> Flow[PipeDriveFile].map(_.req) ~> pipeDriveConnection ~> debugResponse ~> followRedirect ~> s3Connection ~> debugResponse ~> zip.in1
                  bcast ~> zip.in0
      zip.out.map[DownloadedPipeDriveFile] { tp =>
        val pf = tp._1
        val res = tp._2
        println(s"Successful response received for $pf")
        printResponse(res)
        val path = saveToDirectory.resolve(pf.file)
        println(s"Downloading to ${path.toString()}")

        Files.deleteIfExists(path)

        res.entity.dataBytes.runForeach { bs =>
          val fos = Files.newByteChannel(
            path,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND,
            StandardOpenOption.WRITE)
          try {
            fos.write(bs.asByteBuffer)
          } finally {
            fos.close()
          }
        }
        DownloadedPipeDriveFile(tp._1, tp._2)
      } ~> merge
    }

    UniformFanInShape(merge.out, balancer.in)
  }


  val fileListWriter = Files.newBufferedWriter(
      fileList,
      Charset.defaultCharset(),
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.WRITE)

  val saveFileToList = Sink.fold[Int, DownloadedPipeDriveFile](1) { (count, f) =>
    fileListWriter.write(s"${f.pipeDriveFile.file}, ${f.pipeDriveFile.orgName}")
    fileListWriter.newLine()
    count + 1
  }

  val start = Source.single(makeFilesHttpRequest(startAt))

  val downloadGraphComplete = FlowGraph.closed(saveFileToList) { implicit b => end =>
    import akka.stream.scaladsl.FlowGraph.Implicits._

    val download = b.add(downloadGraph)
    val downloadFile = b.add(downloadFileGraph)

    start ~> download.in(0)
    download ~> downloadFile
    downloadFile ~> end.inlet
  }


  val results = downloadGraphComplete.run()

  // this outputs the final result of the stream and shuts down the system
  // however because we have no way of telling the system that the stream
  // is complete above we have to wait until things timeout before the system
  // completes the stream successfully...
  results.onComplete { t =>
    t match {
      case Success(i) => println(s"Successfully downloaded ${i-1} files")
      case Failure(e) => println(s"Download failed $e")
    }

    rootListWriter.flush()
    rootListWriter.close()
    fileListWriter.flush()
    fileListWriter.close()
    Console.flush()

    system.shutdown()
  }

}



object JsonTest extends App {
  case class Foo(i: Int = 10, s: String = "lol")

  val foo = upickle.read[Foo]("{}")             // res1: Foo = Foo(10,lol)


  val foo2 = upickle.read[Foo]("""{"i": 123}""") // res2: Foo = Foo(123,lol)
  println(foo2)

  val files: upickle.Js.Value = upickle.json.read(
    """
      | {"success":true,"data":[{"id":1,"user_id":109687,"deal_id":24,"person_id":null,"org_id":20,"product_id":null,
      | "email_message_id":null,"activity_id":null,"note_id":null,"log_id":null,
      | "add_time":"2013-05-02 18:56:01","update_time":"2014-01-29 08:14:28",
      | "file_name":"BoN-OpenX-Trial-Agreement-ABK-20130501_822261096871367520960.docx",
      | "file_type":"docx","file_size":35387,"active_flag":true,"inline_flag":false,"comment":null,
      | "remote_location":"s3","remote_id":"BoN - OpenX Trial Agreement - ABK - 20130501.docx",
      | "deal_name":"OpenX Deal","person_name":null,"org_name":"OpenX","product_name":null,
      | "url":"https:\/\/app.pipedrive.com\/api\/v1\/files\/1\/download",
      | "name":"BoN - OpenX Trial Agreement - ABK - 20130501.docx"}],
      | "additional_data":{"pagination":{"start":0,"limit":100,"more_items_in_collection":true,"next_start":100}}}
    """.stripMargin)
  println(files)
  println(files("success"))
  println(files("data"))
  println(files("data")(0)("id"))
  println(files("additional_data")("pagination")("more_items_in_collection"))


}
