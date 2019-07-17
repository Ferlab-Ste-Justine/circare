import java.io.File
import java.nio.file.{Files, Paths}

object Walker {
  def walk(dir: String): List[File] = new File(dir).listFiles().toList  //TODO handle subDirs
}
