package com.adidas.analytics.feature

import com.adidas.analytics.algo.GzipDecompressor
import com.adidas.analytics.util.DFSWrapper._
import com.adidas.utils.BaseAlgorithmTest
import org.apache.hadoop.fs.Path
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers._

class GzipDecompressorTest extends AnyFeatureSpec with BaseAlgorithmTest {

  private val paramFileName: String = "params.json"

  private val paramFilePathHDFS: Path = new Path(hdfsRootTestPath, paramFileName)

  private val paramFilePathLocal: Path = new Path(resolveResource(paramFileName))

  private val fileNamesGZip: Seq[String] =
    Seq("data_20180719111849_data_1-3", "data_20180719111849_data_2-3")

  private val fileNamesZip: Seq[String] = Seq("data_20180719111849_data_3-3")

  private val compressedFiles: Seq[String] = fileNamesGZip
    .map(_ + ".gz")
    .map(fileName => resolveResource(fileName, withProtocol = true)) ++ fileNamesZip
    .map(_ + ".zip")
    .map(fileName => resolveResource(fileName, withProtocol = true))

  private val uncompressedFiles: Seq[String] = fileNamesGZip
    .map(fileName => resolveResource(fileName, withProtocol = true)) ++
    fileNamesZip.map(fileName => resolveResource(fileName, withProtocol = true))

  private val testDataPath: Path =
    new Path(hdfsRootTestPath, "test_landing/bi/test_data_table/data")
  private val recursive: Boolean = true

  Feature("Correct execution of GzipDecompressorBytes") {
    Scenario("File names are correct, compressed files are deleted and file size increases") {

      // prepare data
      fs.mkdirs(testDataPath)
      compressedFiles.foreach(location => fs.copyFromLocalFile(new Path(location), testDataPath))

      // checking pre-conditions
      fs.ls(testDataPath, recursive).size shouldEqual 3
      fs.ls(testDataPath, recursive).map(path => fs.getFileStatus(path).getLen)
      fs.ls(testDataPath, recursive)
        .forall(p => p.getName.endsWith(".gz") || p.getName.endsWith(".zip")) shouldBe true
      val baseFileNamesAndSizesBeforeDecompression = fs
        .ls(testDataPath, recursive)
        .map(path =>
          path.getName.substring(0, path.getName.lastIndexOf(".")) -> fs.getFileStatus(path).getLen
        )
        .toMap

      fs.ls(testDataPath, recursive)
        .count(r => !(r.getName.endsWith(".gz") || r.getName.endsWith(".zip"))) shouldEqual 0
      fs.ls(testDataPath, recursive)
        .count(r => r.getName.endsWith(".gz") || r.getName.endsWith(".zip")) shouldEqual 3

      // running the algorithm
      GzipDecompressor(spark, dfs, paramFilePathHDFS.toUri.toString).run()

      // validating results
      fs.ls(testDataPath, recursive).length shouldEqual 3
      fs.ls(testDataPath, recursive)
        .count(r => !(r.getName.endsWith(".gz") || r.getName.endsWith(".zip"))) shouldEqual 3
      fs.ls(testDataPath, recursive)
        .count(r => r.getName.endsWith(".gz") || r.getName.endsWith(".zip")) shouldEqual 0
      val baseFileNamesAndSizesAfterDecompression = fs
        .ls(testDataPath, recursive)
        .map(path =>
          path.getName.substring(0, path.getName.lastIndexOf(".")) -> fs.getFileStatus(path).getLen
        )
        .toMap

      baseFileNamesAndSizesBeforeDecompression.size shouldBe
        baseFileNamesAndSizesAfterDecompression.size
      baseFileNamesAndSizesBeforeDecompression.forall {
        case (k, v) =>
          baseFileNamesAndSizesAfterDecompression.contains(k) &&
            baseFileNamesAndSizesAfterDecompression(k) >= v
      }
    }

    Scenario("Should throw an exception if uncompressed files are already present") {

      // prepare data
      fs.mkdirs(testDataPath)
      uncompressedFiles.foreach(location => fs.copyFromLocalFile(new Path(location), testDataPath))

      // running the algorithm
      val caught = intercept[RuntimeException] {
        GzipDecompressor(spark, dfs, paramFilePathHDFS.toUri.toString).run()
      }

      assert(caught.getMessage.contains("No codec found for file"))
    }

    Scenario("Should throw an exception if directory does not exist") {

      val caught = intercept[RuntimeException] {
        GzipDecompressor(spark, dfs, paramFilePathHDFS.toUri.toString).run()
      }

      assert(caught.getMessage.contains("does not exist"))
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    fs.copyFromLocalFile(paramFilePathLocal, paramFilePathHDFS)
  }
}
