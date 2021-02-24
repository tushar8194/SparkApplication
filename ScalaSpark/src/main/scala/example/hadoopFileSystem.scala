package example

import java.io.FileNotFoundException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path



object hadoopFileSystem {

  def main(args: Array[String]): Unit = {

    val conf = new Configuration()
    val fs= FileSystem.get(conf)

    val src = new Path("sourcePath")
    val dest = new Path("destinationPath")

    if(fs.exists(src)){
      fs.copyToLocalFile(false, src, dest)
       "destinationPath"}
    else if(scala.reflect.io.File("sourcePath").exists)
      "sourcePath"
    else
      new FileNotFoundException("Certificate does not exists")

  }

}
