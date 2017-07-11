package com.futurice.iodf

import java.io.File

import com.futurice.iodf.Utils._
import com.futurice.iodf.df.{IndexConf, IndexedDf}
import com.futurice.iodf.ioseq.DenseIoBits
import com.futurice.iodf.ml.Knn
import com.futurice.iodf.store.MMapDir
import com.futurice.iodf.util.{LBits, Tracing}
import com.futurice.testtoys.TestSuite

case class Animal(legs:Int, features:String, noise:Boolean, isDuck:Boolean)

case class Article(articleId:Int, features:String)
case class Decision(articleId:Int, features:String, hit:Boolean)

/**
  * Created by arau on 22.3.2017.
  */
class MlTest extends TestSuite("ml") {


  val items =
    Seq(Animal(2, "yellow walks", true, false),
        Animal(4, "moo white walks", false, false),
        Animal(4, "moo black walks", true, false),
        Animal(2, "flies white swims", false, false),
        Animal(2, "flies yellow swims", true, true),
        Animal(2, "flies yellow swims", false, true))
  val articles =
    Seq(Article(0, "lappi ivalo kotimaa"),
        Article(1, "lappi rovaniemi kotimaa"),
        Article(2, "lappi rovaniemi urheilu"),
        Article(3, "ulkomaat trump"),
        Article(4, "ulkomaat eu"))
  val decisions =
    Seq(
      Decision(0, "lappi ivalo kotimaa", true),
      Decision(0, "lappi ivalo kotimaa", true),
      Decision(0, "lappi ivalo kotimaa", false),
      Decision(1, "lappi rovaniemi kotimaa", true),
      Decision(1, "lappi rovaniemi kotimaa", true),
      Decision(1, "lappi rovaniemi kotimaa", true),
      Decision(2, "lappi rovaniemi urheilu", true),
      Decision(2, "lappi rovaniemi urheilu", false),
      Decision(2, "lappi rovaniemi urheilu", true),
      Decision(3, "ulkomaat trump", false),
      Decision(3, "ulkomaat trump", true),
      Decision(3, "ulkomaat trump", false),
      Decision(4, "ulkomaat eu", false),
      Decision(4, "ulkomaat eu", false),
      Decision(4, "ulkomaat eu", false))

  val indexConf =
    IndexConf[String]().withAnalyzer("features", e => e.asInstanceOf[String].split(" ").toSeq)

  test("knn") { t =>
    Tracing.trace {
      scoped { implicit bind =>
        val dir = bind(new MMapDir(t.fileDir))
        val df = bind(IndexedDf[Animal](items, indexConf))
        implicit val io = IoContext()
        val knn =
          Knn(
            df,
            indexConf,
            Set("legs", "features"),
            "isDuck" -> true,
            0.0)
        def tAnimal(a:Animal) {
          t.tln("knn for: " + a)
          knn.knn(10, a).foreach { case (d, i) =>
            t.tln("  " + d + ": " + df(i))
          }
          t.tln
        }
        tAnimal(Animal(4, "flies white swims", true, true))
        tAnimal(Animal(4, "flies yellow swims", true, false))
      }
    }
  }

  test("knn-weights") { t =>
    Tracing.trace {
      scoped { implicit bind =>
        implicit val io = IoContext()
        val dir = bind(new MMapDir(new File(t.fileDir, "articles")))
        val df = bind(IndexedDf[Article](articles, indexConf))
        val dir2 = bind(new MMapDir(new File(t.fileDir, "decisions")))
        val df2 = bind(IndexedDf[Decision](decisions, indexConf))
        val weights =
          Knn.keyValueWeights(
            df2,
            Set("features"),
            "hit" -> true,
            0.0)

        t.tln("based on hit dataframe, key-value weights are:")
        weights.foreach { case ((key,value), w) =>
          t.tln(f"  ${w._1}%.3f/${w._2}%.3f $key=$value")
        }
        t.tln
        val knn =
          new Knn(
            df,
            LBits.from(0L until df.lsize map (e => true)),
            indexConf,
            weights)

        val hits = df2.index("hit" -> true)
        def articleCtr(i:Int) = {
          using (df2.openIndex("articleId" -> i)) { bits =>
            val f = bits.fAnd(hits)
            val n = bits.f
            (f+1)/(n+2).toDouble
          }
        }

        def tArticle(a:Article) {
          t.tln("knn for: " + a)
          var ctrSum = 0.0
          val k = 2
          knn.knn(k, a).foreach { case (d, i) =>
            val article = df(i)
            val ctr = articleCtr(article.articleId)
            ctrSum += ctr
            t.tln("  distance:" + d + " : " + df(i) + " ctr:" + ctr)
          }
          t.tln(f"-> ctr estimate: ${ctrSum / k.toDouble}%.3f")
          t.tln
        }
        tArticle(Article(5, "lappi ivalo urheilu"))
        tArticle(Article(6, "lappi rovaniemi trump"))
        tArticle(Article(7, "lappi"))
        tArticle(Article(8, "ulkomaat"))
        tArticle(Article(9, "ulkomaat eu"))
      }
    }
  }

}
