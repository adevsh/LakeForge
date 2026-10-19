package lakeforge

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, StandardCopyOption}
import java.sql.{Connection, DriverManager, ResultSet}
import java.time.Instant
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}
import scala.util.control.NonFatal
import ujson.{Obj, Value}

object Policy {
  val states = Set("QUEUED", "STARTING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT")
  val terminal = Set("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT")
  val transitions = Map(
    "QUEUED" -> Set("STARTING", "CANCELLED", "FAILED"),
    "STARTING" -> Set("RUNNING", "CANCELLED", "FAILED", "TIMED_OUT"),
    "RUNNING" -> terminal
  )
  def canMove(from: String, to: String): Boolean = transitions.getOrElse(from, Set.empty).contains(to)
  def sqlValid(sql: String): Boolean = {
    val text = sql.trim
    text.matches("(?is)^select\\b[\\s\\S]*") &&
      !text.contains(";") && !text.contains("--") && !text.contains("/*") &&
      text.matches("(?is)[\\s\\S]*\\bfrom\\s+input\\b[\\s\\S]*") && text.length <= 10000
  }
  def fileValid(name: String): Boolean = name.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\\.parquet") &&
    !name.contains("..")
}

final case class Limits(cpus: Double, memory: String, timeout: Int, concurrency: Int, maxOutputBytes: Long)
object Limits {
  def from(env: Map[String, String]): Limits = {
    val cpus = env.getOrElse("LAKEFORGE_CPUS", "1").toDouble
    val memory = env.getOrElse("LAKEFORGE_MEMORY", "1g")
    val timeout = env.getOrElse("LAKEFORGE_TIMEOUT_SECONDS", "120").toInt
    val concurrency = env.getOrElse("LAKEFORGE_CONCURRENCY", "2").toInt
    val maxOutput = env.getOrElse("LAKEFORGE_MAX_OUTPUT_BYTES", "268435456").toLong
    require(cpus >= 0.1 && cpus <= 8 && !cpus.isNaN && !cpus.isInfinity, "CPU limit must be 0.1 to 8")
    require(memory.matches("[1-9][0-9]{0,3}[mg]"), "memory limit must be e.g. 512m or 1g")
    require(timeout >= 1 && timeout <= 3600, "timeout must be 1 to 3600 seconds")
    require(concurrency >= 1 && concurrency <= 8, "concurrency must be 1 to 8")
    require(maxOutput >= 1024 && maxOutput <= 1073741824L, "output limit must be 1 KiB to 1 GiB")
    Limits(cpus, memory, timeout, concurrency, maxOutput)
  }
}

final class Store(root: Path) {
  Files.createDirectories(root)
  private val db = root.resolve("lakeforge.sqlite")
  Class.forName("org.sqlite.JDBC")
  private val connection = DriverManager.getConnection(s"jdbc:sqlite:${db.toAbsolutePath}")
  private def statement(sql: String): Unit = {
    val stmt = connection.createStatement()
    try stmt.execute(sql) finally stmt.close()
  }
  private def migrate(resourceName: String, target: Int): Unit = {
    val resource = getClass.getResourceAsStream(s"/db/$resourceName")
    require(resource != null, s"missing database migration $resourceName")
    val sql = try new String(resource.readAllBytes(), UTF_8) finally resource.close()
    connection.setAutoCommit(false)
    try {
      sql.split(";").map(_.trim).filter(_.nonEmpty).foreach(statement)
      statement(s"PRAGMA user_version=$target")
      connection.commit()
    } catch { case NonFatal(e) => connection.rollback(); throw e }
    finally connection.setAutoCommit(true)
  }
  statement("PRAGMA journal_mode=WAL")
  statement("PRAGMA foreign_keys=ON")
  private val versionStatement = connection.createStatement()
  private val version = versionStatement.executeQuery("PRAGMA user_version")
  val current = try { version.next(); version.getInt(1) }
    finally { version.close(); versionStatement.close() }
  if (current < 1) migrate("V1__initial.sql", 1)
  if (current < 2) migrate("V2__dataset_schema.sql", 2)
  if (current < 3) migrate("V3__image_digest.sql", 3)

  private def query(sql: String, args: Seq[String] = Seq.empty)(f: ResultSet => Value): Seq[Value] = {
    val statement = connection.prepareStatement(sql)
    try {
      args.zipWithIndex.foreach { case (arg, i) => statement.setString(i + 1, arg) }
      val results = statement.executeQuery()
      try {
        val values = Vector.newBuilder[Value]
        while (results.next()) values += f(results)
        values.result()
      } finally results.close()
    } finally statement.close()
  }
  private def execute(sql: String, args: Seq[String]): Int = {
    val statement = connection.prepareStatement(sql)
    try {
      args.zipWithIndex.foreach { case (arg, i) => statement.setString(i + 1, arg) }
      statement.executeUpdate()
    } finally statement.close()
  }
  private def nullable(value: String): Value = if (value == null) ujson.Null else ujson.Str(value)
  private def dataset(rs: ResultSet): Value =
    Obj("id" -> rs.getString("id"), "name" -> rs.getString("name"),
      "file" -> rs.getString("file"), "createdAt" -> rs.getString("created_at"),
      "schema" -> (if (rs.getString("schema_json") == null) ujson.Null else ujson.read(rs.getString("schema_json"))),
      "byteSize" -> (if (rs.getObject("byte_size") == null) ujson.Null else ujson.Num(rs.getLong("byte_size"))),
      "rowCount" -> (if (rs.getObject("row_count") == null) ujson.Null else ujson.Num(rs.getLong("row_count"))))
  private def job(rs: ResultSet): Value = {
    val started = rs.getString("started_at")
    val finished = rs.getString("finished_at")
    val duration = if (started != null && finished != null)
      ujson.Num(java.time.Duration.between(Instant.parse(started), Instant.parse(finished)).toMillis)
    else ujson.Null
    Obj("id" -> rs.getString("id"), "status" -> rs.getString("status"),
      "datasetId" -> rs.getString("dataset_id"), "sql" -> rs.getString("sql_text"),
      "engine" -> rs.getString("engine"), "mode" -> rs.getString("mode"),
      "image" -> rs.getString("image"), "imageDigest" -> nullable(rs.getString("image_digest")),
      "submittedAt" -> rs.getString("submitted_at"),
      "startedAt" -> nullable(started), "finishedAt" -> nullable(finished), "durationMs" -> duration,
      "containerId" -> nullable(rs.getString("container_id")),
      "exitCode" -> (if (rs.getObject("exit_code") == null) ujson.Null else ujson.Num(rs.getInt("exit_code"))),
      "exitReason" -> nullable(rs.getString("exit_reason")), "failure" -> nullable(rs.getString("failure")),
      "outputFile" -> nullable(rs.getString("output_file")),
      "result" -> (if (rs.getString("result_json") == null) ujson.Null else ujson.read(rs.getString("result_json"))),
      "traceId" -> rs.getString("trace_id"), "cpuLimit" -> rs.getDouble("cpu_limit"),
      "memoryLimit" -> rs.getString("memory_limit"), "timeoutSeconds" -> rs.getInt("timeout_seconds"),
      "cpuMeasured" -> ujson.Null, "memoryMeasured" -> ujson.Null, "gpuMeasured" -> ujson.Null)
  }
  def datasets: Seq[Value] = synchronized { query("SELECT * FROM datasets ORDER BY created_at DESC")(dataset) }
  def getDataset(id: String): Option[Value] = synchronized {
    query("SELECT * FROM datasets WHERE id=?", Seq(id))(dataset).headOption
  }
  def register(name: String, file: String, inspection: Value): Value = synchronized {
    val id = UUID.randomUUID().toString
    val now = Instant.now().toString
    execute("""INSERT INTO datasets(id,name,file,created_at,schema_json,byte_size,row_count)
      VALUES(?,?,?,?,?,?,?)""",
      Seq(id, name, file, now, inspection("columns").render(),
        inspection("byteSize").num.toLong.toString, inspection("rowCount").num.toLong.toString))
    getDataset(id).get
  }
  def jobs: Seq[Value] = synchronized { query("SELECT * FROM jobs ORDER BY submitted_at DESC")(job) }
  def getJob(id: String): Option[Value] = synchronized { query("SELECT * FROM jobs WHERE id=?", Seq(id))(job).headOption }
  def createJob(datasetId: String, sql: String, image: String, digest: String, limits: Limits): Value = synchronized {
    val id = UUID.randomUUID().toString
    execute("""INSERT INTO jobs(id,status,dataset_id,sql_text,engine,mode,image,image_digest,submitted_at,trace_id,cpu_limit,memory_limit,timeout_seconds)
      VALUES(?,'QUEUED',?,?,'duckdb','cpu',?,?,?,?,?,?,?)""",
      Seq(id, datasetId, sql, image, digest, Instant.now().toString,
        UUID.randomUUID().toString.replace("-", ""), limits.cpus.toString, limits.memory, limits.timeout.toString))
    getJob(id).get
  }
  def move(id: String, to: String, fields: Map[String, String] = Map.empty): Boolean = synchronized {
    require(Policy.states(to))
    val current = getJob(id).map(_("status").str)
    if (!current.exists(Policy.canMove(_, to))) false
    else {
      val allowed = Set("container_id", "exit_reason", "failure", "output_file", "result_json")
      require(fields.keys.forall(allowed))
      val withTime = fields ++ (if (to == "RUNNING") Map("started_at" -> Instant.now().toString)
        else if (Policy.terminal(to)) Map("finished_at" -> Instant.now().toString)
        else Map.empty[String, String])
      val assignments = (Seq("status=?") ++ withTime.keys.toSeq.map(_ + "=?")).mkString(",")
      execute(s"UPDATE jobs SET $assignments WHERE id=?", Seq(to) ++ withTime.values.toSeq ++ Seq(id))
      true
    }
  }
  def container(id: String, containerId: String): Unit = synchronized {
    execute("UPDATE jobs SET container_id=? WHERE id=?", Seq(containerId, id))
  }
  def exit(id: String, code: Int, reason: String): Unit = synchronized {
    execute("""UPDATE jobs SET exit_code=?,
      exit_reason=CASE WHEN exit_reason IN ('USER_CANCELLED','RESTART') THEN exit_reason ELSE ? END
      WHERE id=?""", Seq(code.toString, reason, id))
  }
  def close(): Unit = synchronized { connection.close() }
}

object Runtime {
  def select(configured: Option[String], available: String => Boolean): String =
    configured.getOrElse(if (available("podman")) "podman" else "docker")

  def onPath(name: String): Boolean =
    sys.env.getOrElse("PATH", "").split(java.io.File.pathSeparator).exists { directory =>
      Files.isExecutable(Path.of(directory, name))
    }
}

final class Runtime(command: String) {
  def call(args: Seq[String], timeout: Int = 30): (Int, String) = {
    val process = new ProcessBuilder((command +: args): _*).redirectErrorStream(true).start()
    if (!process.waitFor(timeout, TimeUnit.SECONDS)) {
      process.destroyForcibly()
      throw new IllegalStateException(s"$command ${args.headOption.getOrElse("")} timed out")
    }
    (process.exitValue(), new String(process.getInputStream.readAllBytes(), UTF_8).trim)
  }
  def checked(args: Seq[String]): String = {
    val (code, output) = call(args)
    if (code != 0) throw new IllegalStateException(s"runtime ${args.head}: $output")
    output
  }
  def stop(id: String): Unit = { call(Seq("stop", "-t", "2", id), 15); () }
  def remove(id: String): Unit = { call(Seq("rm", "-f", id), 15); () }
}

final class Service(val root: Path, val store: Store, runtime: Runtime, image: String, limits: Limits) {
  private val pool = Executors.newFixedThreadPool(limits.concurrency)
  Seq("inputs", "staging", "outputs", "logs").foreach(name => Files.createDirectories(root.resolve(name)))

  def inspect(input: Path): Value = {
    val args = Seq("run", "--rm", "--network", "none", "--read-only",
      "--security-opt", "no-new-privileges", "--cap-drop", "ALL", "--cpus", limits.cpus.toString,
      "--memory", limits.memory, "-v", s"${input.toAbsolutePath}:/inputs/input.parquet:ro",
      "--entrypoint", "python", image, "/app/inspect.py")
    val value = ujson.read(runtime.checked(args))
    require(value("columns").arr.nonEmpty && value("byteSize").num > 0 &&
      value("rowCount").num >= 0, "invalid Parquet inspection")
    value
  }

  def recover(): Unit = store.jobs.filter(j => !Policy.terminal(j("status").str)).foreach { j =>
    val id = j("id").str
    // The stable name covers a crash between container launch and ID persistence.
    val runtimeId = if (j("containerId") == ujson.Null) s"lakeforge-$id" else j("containerId").str
    try runtime.stop(runtimeId) catch { case NonFatal(_) => () }
    try {
      val (_, output) = runtime.call(Seq("logs", runtimeId))
      if (output.nonEmpty) log(id, output + "\n")
    } catch { case NonFatal(_) => () }
    try runtime.remove(runtimeId) catch { case NonFatal(_) => () }
    store.move(id, "FAILED", Map("failure" -> "Control plane restarted during execution", "exit_reason" -> "RESTART"))
  }

  def submit(dataset: Value, sql: String): Value = {
    val digest = runtime.checked(Seq("image", "inspect", "--format", "{{.Id}}", image)).trim
    require(digest.matches("sha256:[0-9a-f]{64}"), "worker image digest unavailable")
    val job = store.createJob(dataset("id").str, sql, image, digest, limits)
    pool.execute(() => run(job("id").str, dataset("file").str, sql, digest))
    job
  }
  def cancel(id: String): Value = {
    val job = store.getJob(id).getOrElse(throw new NoSuchElementException("job not found"))
    if (!Policy.terminal(job("status").str)) {
      store.move(id, "CANCELLED", Map("failure" -> "Cancelled by user", "exit_reason" -> "USER_CANCELLED"))
      log(id, s"Job $id cancelled by user\n")
      store.getJob(id).foreach { updated =>
        if (updated("containerId") != ujson.Null) {
          try runtime.stop(updated("containerId").str) catch { case NonFatal(_) => () }
        }
      }
    }
    store.getJob(id).get
  }
  private def log(id: String, text: String): Unit = {
    Files.writeString(root.resolve("logs").resolve(s"$id.log"), text, UTF_8)
  }
  def logs(id: String): String = {
    if (store.getJob(id).isEmpty) throw new NoSuchElementException("job not found")
    val path = root.resolve("logs").resolve(s"$id.log")
    if (Files.exists(path)) Files.readString(path, UTF_8) else "Logs pending"
  }
  private def run(id: String, file: String, sql: String, digest: String): Unit = {
    var container: Option[String] = None
    var logsCaptured = false
    val work = root.resolve("staging").resolve(id)
    def captureLogs(): Unit = if (!logsCaptured) {
      container.foreach { cid =>
        try {
          val (_, output) = runtime.call(Seq("logs", cid))
          val previous = root.resolve("logs").resolve(s"$id.log")
          val prefix = if (Files.exists(previous)) Files.readString(previous, UTF_8) else ""
          log(id, prefix + "\n" + output)
        } catch { case NonFatal(e) => log(id, s"log collection failed: ${e.getMessage}") }
        logsCaptured = true
      }
    }
    try {
      if (!store.move(id, "STARTING")) return
      Files.createDirectories(work)
      Files.writeString(work.resolve("request.json"),
        Obj("version" -> 1, "jobId" -> id, "sql" -> sql).render(), UTF_8)
      val input = root.resolve("inputs").resolve(file).toRealPath()
      require(input.startsWith(root.resolve("inputs").toRealPath()), "input outside registered directory")
      val uid = sys.env.getOrElse("UID", new ProcessBuilder("id", "-u").start().inputReader().readLine())
      val gid = new ProcessBuilder("id", "-g").start().inputReader().readLine()
      val args = Seq("run", "-d", "--name", s"lakeforge-$id", "--network", "none",
        "--read-only", "--security-opt", "no-new-privileges", "--cap-drop", "ALL",
        "--pids-limit", "128", "--cpus", limits.cpus.toString, "--memory", limits.memory,
        "--tmpfs", "/tmp:rw,nosuid,size=64m", "--user", s"$uid:$gid",
        "-v", s"${input.toAbsolutePath}:/inputs/input.parquet:ro",
        "-v", s"${work.toAbsolutePath}:/work:rw", digest)
      val containerId = runtime.checked(args).linesIterator.next().trim
      container = Some(containerId)
      store.container(id, containerId)
      if (!store.move(id, "RUNNING")) { runtime.stop(containerId); return }
      val (waitCode, waitOutput) = runtime.call(Seq("wait", containerId), limits.timeout)
      if (waitCode != 0) throw new IllegalStateException(s"container wait failed: $waitOutput")
      captureLogs()
      val exitCode = waitOutput.linesIterator.nextOption().flatMap(_.trim.toIntOption).getOrElse(-1)
      val reason = try {
        if (runtime.checked(Seq("inspect", "--format", "{{.State.OOMKilled}}", containerId)).trim == "true") "OOM"
        else if (exitCode == 0) "EXITED" else "NONZERO_EXIT"
      } catch { case NonFatal(_) => "UNKNOWN" }
      store.exit(id, exitCode, reason)
      if (store.getJob(id).exists(j => Policy.terminal(j("status").str))) return
      if (exitCode != 0) throw new IllegalStateException(s"Worker exited $exitCode ($reason)")
      val result = ujson.read(Files.readString(work.resolve("result.json"), UTF_8))
      require(result("version").num == 1 && result("jobId").str == id &&
        result("rowCount").num >= 0 && result("columns").arr.nonEmpty, "invalid worker result")
      val output = work.resolve("output.parquet")
      require(Files.isRegularFile(output) && Files.size(output) > 0, "missing Parquet output")
      require(Files.size(output) <= limits.maxOutputBytes, "output exceeds configured limit")
      val published = root.resolve("outputs").resolve(s"$id.parquet")
      Files.move(output, published, StandardCopyOption.ATOMIC_MOVE)
      store.move(id, "SUCCEEDED", Map("output_file" -> published.toString, "result_json" -> result.render()))
    } catch {
      case NonFatal(e) =>
        val timedOut = Option(e.getMessage).exists(_.contains("timed out"))
        if (timedOut) container.foreach(cid => try runtime.stop(cid) catch { case NonFatal(_) => () })
        captureLogs()
        try {
          val path = root.resolve("logs").resolve(s"$id.log")
          log(id, (if (Files.exists(path)) Files.readString(path, UTF_8) else "") + "\n" + e.toString)
        } catch { case NonFatal(_) => () }
        val status = store.getJob(id).map(_("status").str).getOrElse("FAILED")
        if (!Policy.terminal(status)) {
          val existingReason = store.getJob(id).flatMap(j =>
            if (j("exitReason") == ujson.Null) None else Some(j("exitReason").str))
          store.move(id, if (timedOut) "TIMED_OUT" else "FAILED",
            Map("failure" -> Option(e.getMessage).getOrElse(e.toString),
              "exit_reason" -> existingReason.getOrElse(
                if (timedOut) "TIME_LIMIT" else if (container.isEmpty) "START_FAILED" else "WORKER_ERROR")))
        }
    } finally {
      container.foreach { cid =>
        captureLogs()
        try runtime.remove(cid) catch { case NonFatal(_) => () }
      }
    }
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val root = Path.of(sys.env.getOrElse("LAKEFORGE_DATA", "data")).toAbsolutePath.normalize()
    val runtime = new Runtime(Runtime.select(sys.env.get("LAKEFORGE_RUNTIME"), Runtime.onPath))
    val image = sys.env.getOrElse("LAKEFORGE_DUCKDB_IMAGE", "lakeforge-duckdb:1.0.0")
    val limits = Limits.from(sys.env)
    val store = new Store(root)
    val service = new Service(root, store, runtime, image, limits)
    service.recover()
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1",
      sys.env.getOrElse("LAKEFORGE_PORT", "8080").toInt), 0)
    server.createContext("/api", exchange => {
      try handle(exchange, root, store, service)
      catch {
        case e: NoSuchElementException => respond(exchange, 404, Obj("error" -> e.getMessage))
        case e: IllegalArgumentException => respond(exchange, 400, Obj("error" -> Option(e.getMessage).getOrElse("invalid request")))
        case NonFatal(e) =>
          System.err.println(s"request failed: ${e.toString}")
          respond(exchange, 500, Obj("error" -> "Internal server error"))
      }
    })
    server.setExecutor(Executors.newFixedThreadPool(4))
    server.start()
    println(s"LakeForge listening on http://127.0.0.1:${server.getAddress.getPort}")
  }
  private def body(exchange: HttpExchange): Value =
    ujson.read(new String(exchange.getRequestBody.readNBytes(16384), UTF_8))
  private def respond(exchange: HttpExchange, status: Int, value: Value): Unit = {
    val bytes = value.render().getBytes(UTF_8)
    exchange.getResponseHeaders.set("Content-Type", "application/json; charset=utf-8")
    exchange.sendResponseHeaders(status, bytes.length)
    val output = exchange.getResponseBody
    try output.write(bytes) finally output.close()
  }
  private def text(exchange: HttpExchange, value: String): Unit = {
    val bytes = value.getBytes(UTF_8)
    exchange.getResponseHeaders.set("Content-Type", "text/plain; charset=utf-8")
    exchange.sendResponseHeaders(200, bytes.length)
    val output = exchange.getResponseBody
    try output.write(bytes) finally output.close()
  }
  private def handle(e: HttpExchange, root: Path, store: Store, service: Service): Unit = {
    val path = e.getRequestURI.getPath.stripPrefix("/api").split("/").filter(_.nonEmpty).toList
    (e.getRequestMethod, path) match {
      case ("GET", List("health")) => respond(e, 200, Obj("ok" -> true))
      case ("GET", List("datasets")) => respond(e, 200, ujson.Arr.from(store.datasets))
      case ("POST", List("datasets")) =>
        val request = body(e)
        val name = request("name").str.trim
        val file = request("file").str
        require(name.nonEmpty && name.length <= 80 && Policy.fileValid(file), "invalid name or Parquet file")
        val inputDir = root.resolve("inputs").toRealPath()
        val input = inputDir.resolve(file)
        require(Files.isRegularFile(input) && input.toRealPath().startsWith(inputDir), "file must exist inside inputs")
        respond(e, 201, store.register(name, file, service.inspect(input.toRealPath())))
      case ("GET", List("jobs")) => respond(e, 200, ujson.Arr.from(store.jobs))
      case ("POST", List("jobs")) =>
        val request = body(e)
        require(request("engine").str == "duckdb" && request("mode").str == "cpu", "unsupported engine or mode")
        val sql = request("sql").str
        require(Policy.sqlValid(sql), "only one SELECT statement is supported")
        val dataset = store.getDataset(request("datasetId").str)
          .getOrElse(throw new IllegalArgumentException("unknown dataset"))
        respond(e, 202, service.submit(dataset, sql))
      case ("GET", List("jobs", id)) => respond(e, 200, store.getJob(id)
        .getOrElse(throw new NoSuchElementException("job not found")))
      case ("GET", List("jobs", id, "logs")) => text(e, service.logs(id))
      case ("POST", List("jobs", id, "cancel")) => respond(e, 200, service.cancel(id))
      case _ => respond(e, 404, Obj("error" -> "not found"))
    }
  }
}
