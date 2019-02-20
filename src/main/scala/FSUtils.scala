import org.apache.hadoop.fs.Path

object FSUtils {

  implicit class PathExt(val p: Path) {
    def resolve(child: String): Path = {
      new Path(p, child)
    }

    def resolve(child: Path): Path = {
      new Path(p, child)
    }
  }

}
