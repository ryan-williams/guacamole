package org.hammerlab.guacamole.util

import java.io.{InputStream, OutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

trait Saveable {
  def save(os: OutputStream): Unit

  def save(sc: SparkContext, fn: String): Unit = save(sc.hadoopConfiguration, new Path(fn))
  def save(sc: SparkContext, path: Path): Unit = save(sc.hadoopConfiguration, path)
  def save(hc: Configuration, fn: String): Unit = save(hc, new Path(fn))
  def save(hc: Configuration, path: Path): Unit = save(hc, path, overwrite = true)

  def save(sc: SparkContext, fn: String, overwrite: Boolean): Unit = save(sc.hadoopConfiguration, new Path(fn), overwrite)
  def save(sc: SparkContext, path: Path, overwrite: Boolean): Unit = save(sc.hadoopConfiguration, path, overwrite)
  def save(hc: Configuration, fn: String, overwrite: Boolean): Unit = save(hc, new Path(fn), overwrite)
  def save(hc: Configuration, path: Path, overwrite: Boolean): Unit = {
    val fs = FileSystem.get(hc)
    if (!fs.exists(path) || overwrite) {
      val os = fs.create(path)
      save(os)
      os.close()
    } else {
      println(s"Skipping writing: $path")
    }
  }
}

//abstract class Loadable[T] {
//  final def load(is: InputStream): T = {
//    val t = _load(is)
//    is.close()
//    t
//  }
//  protected def _load(is: InputStream): T
//  def load(sc: SparkContext, fn: String): T = load(sc.hadoopConfiguration, new Path(fn))
//  def load(sc: SparkContext, path: Path): T = load(sc.hadoopConfiguration, path)
//  def load(hc: Configuration, fn: String): T = load(hc, new Path(fn))
//  def load(hc: Configuration, path: Path): T = load(FileSystem.get(hc).open(path))
//}
