//> using test.dep org.scalameta::munit:1.1.0
package lakeforge

import java.nio.file.Files

class PolicyTest extends munit.FunSuite {
  test("allowed transitions forbid revival from terminal states") {
    assert(Policy.canMove("QUEUED", "STARTING"))
    assert(Policy.canMove("RUNNING", "SUCCEEDED"))
    assert(!Policy.canMove("SUCCEEDED", "RUNNING"))
    assert(!Policy.canMove("CANCELLED", "SUCCEEDED"))
  }
  test("job SQL and registered file names are bounded") {
    assert(Policy.sqlValid("SELECT id FROM input"))
    assert(!Policy.sqlValid("DELETE FROM input"))
    assert(!Policy.sqlValid("SELECT * FROM read_parquet('/etc/passwd')"))
    assert(!Policy.sqlValid("SELECT * FROM input; DROP TABLE jobs"))
    assert(!Policy.fileValid("../input.parquet"))
    assert(Policy.fileValid("sample.parquet"))
  }
  test("resource limits reject unreasonable settings") {
    assertEquals(Limits.from(Map.empty).memory, "1g")
    intercept[IllegalArgumentException](Limits.from(Map("LAKEFORGE_CPUS" -> "0")))
    intercept[IllegalArgumentException](Limits.from(Map("LAKEFORGE_MEMORY" -> "unlimited")))
    intercept[IllegalArgumentException](Limits.from(Map("LAKEFORGE_CONCURRENCY" -> "9")))
  }
  test("runtime prefers Podman but uses Docker when Podman is unavailable") {
    assertEquals(Runtime.select(None, _ == "podman"), "podman")
    assertEquals(Runtime.select(None, _ => false), "docker")
    assertEquals(Runtime.select(Some("docker"), _ == "podman"), "docker")
  }
  test("SQLite job metadata survives a new store instance") {
    val root = Files.createTempDirectory("lakeforge-store-test")
    val first = new Store(root)
    val dataset = first.register("Sample", "sample.parquet", ujson.Obj(
      "columns" -> ujson.Arr(ujson.Obj("name" -> "id", "type" -> "BIGINT")),
      "byteSize" -> 100, "rowCount" -> 10))
    val job = first.createJob(dataset("id").str, "SELECT * FROM input",
      "lakeforge-duckdb:1.0.0", "sha256:" + "a" * 64, Limits.from(Map.empty))
    val id = job("id").str
    assert(first.move(id, "STARTING"))
    assert(first.move(id, "RUNNING"))
    assert(first.move(id, "FAILED", Map("failure" -> "worker unavailable")))
    assert(!first.move(id, "SUCCEEDED"))
    first.close()
    val reopened = new Store(root)
    assertEquals(reopened.getDataset(dataset("id").str).get("rowCount").num.toInt, 10)
    assertEquals(reopened.getJob(id).get("status").str, "FAILED")
    assertEquals(reopened.getJob(id).get("failure").str, "worker unavailable")
    assertEquals(reopened.getJob(id).get("imageDigest").str, "sha256:" + "a" * 64)
    reopened.close()
  }
}
