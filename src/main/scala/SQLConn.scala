import java.sql.{Connection, Driver, DriverManager, ResultSet, SQLException}
import java.util.Properties

import play.api.libs.json.{JsObject, JsString, JsValue, Json}

import scala.collection.immutable

class SQLConn(host: String, user: String, pass: String) {
  private val con: Connection = {

    val props: Properties = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", pass)
    props.setProperty("connectTimeout", "10")

    DriverManager.getConnection(s"jdbc:postgresql://$host/kfpostgresprd?user=$user&password=$pass")
  }

  /**
    * "Scala-izes" a JBDC driver query, transforming the resulting ResultSet into an iterator of Maps of String, String
    *
    * @param strStatement The statement to execute. Will be sanitized to reduce connection overhead
    * @return
    */
  private def execute(strStatement: String): Iterator[immutable.IndexedSeq[JsValue]] = {
    val results = con.prepareStatement(strStatement.replaceAll("\\s+", " ")).executeQuery()

    val meta = results.getMetaData

    Iterator.continually(results.next()).takeWhile(identity).map{ _ =>
      for(i <- 1 to meta.getColumnCount) yield Json.parse(results.getObject(i).toString)
    }
  }


  private def printtab(str: Object): Unit = print(str+"\t")

  def printPDFAndParticipants(): Unit = {
    getPDFAndParticipants().foreach{ a =>
      a.foreach(printtab(_))
      println()
    }
  }

  def printCount(): Unit = {
    var i=0
    getPDFAndParticipants().foreach(_ => i+=1)
    println("Results have "+i+"rows")
  }

  def getPDFAndParticipants(): Iterator[immutable.IndexedSeq[JsValue]] = {
    execute(
      """
        |WITH
        |    participants as (
        |        SELECT kf_id, external_id as pt_external_id, family_id, is_proband, ethnicity, gender, study_id, affected_status, diagnosis_category
        |        FROM participant
        |        WHERE study_id='SD_BHJXBDQK'
        |    ),
        |    sp_file as (
        |        SELECT sp_id, genomic_file_id, participant_id
        |        FROM
        |            (SELECT biospecimen.kf_id as sp_id, participant_id FROM biospecimen, participants WHERE participant_id=participants.kf_id) as specimens_id,
        |            biospecimen_genomic_file
        |        WHERE specimens_id.sp_id=biospecimen_genomic_file.biospecimen_id
        |    ),
        |    bs as (
        |
        |        WITH gf as (
        |
        |            SELECT
        |                sp_id,
        |                jsonb_agg(to_jsonb(genomic_file.*)) as genomic_file
        |            FROM
        |                sp_file,
        |                genomic_file
        |            WHERE genomic_file.kf_id=genomic_file_id
        |            GROUP BY sp_id
        |
        |        )
        |
        |        SELECT participant_id, to_jsonb(sp_filejson) as biospecimen
        |        FROM
        |            (SELECT biospecimen.*, genomic_file
        |            FROM biospecimen, gf
        |            WHERE gf.sp_id=biospecimen.kf_id) as sp_filejson
        |
        |    ),
        |    dg as (
        |        SELECT participant_id, jsonb_agg(to_jsonb(diagnosis)) as diagnosis
        |        FROM diagnosis, participants
        |        WHERE participant_id=participants.kf_id
        |        GROUP BY participant_id
        |    ),
        |    ph as (
        |        SELECT participant_id, jsonb_agg(to_jsonb(phenotype)) as phenotype
        |        FROM phenotype, participants
        |        WHERE participant_id=participants.kf_id
        |        GROUP BY participant_id
        |    ),
        |    ou as (
        |        SELECT participant_id, to_jsonb(outcome) as outcome
        |        FROM outcome, participants
        |        WHERE participant_id=participants.kf_id
        |    ),
        |    bs_dg_ph_ou as (
        |        SELECT *
        |        FROM
        |            bs
        |            NATURAL FULL JOIN
        |            dg
        |            NATURAL FULL JOIN
        |            ph
        |            NATURAL FULL JOIN
        |            ou
        |    ),
        |    pdf as (
        |
        |        SELECT participant_id, COALESCE(pdf_agg.jsonb_agg, '[]'::jsonb) as pdf
        |        FROM
        |            (SELECT participant_id, jsonb_agg(jsonb_build_object('pdf_key', genomic_file.external_id, 'pdf_id', genomic_file.kf_id, 'pdf_type', genomic_file.data_type))
        |            FROM genomic_file, sp_file
        |            WHERE file_format='pdf' AND sp_file.genomic_file_id=genomic_file.kf_id
        |            GROUP BY participant_id) as pdf_agg,
        |            participants
        |        WHERE participants.kf_id=participant_id
        |    )
        |
        |    SELECT pdf.pdf, to_jsonb(merged) as json
        |    FROM
        |        (SELECT *
        |        FROM participants LEFT JOIN bs_dg_ph_ou
        |        ON participants.kf_id=bs_dg_ph_ou.participant_id) as merged,
        |        (SELECT *
        |        FROM participants LEFT JOIN pdf
        |        ON participants.kf_id=pdf.participant_id) as pdf
        |    WHERE merged.participant_id=pdf.participant_id
      """.stripMargin
    )
  }

}

