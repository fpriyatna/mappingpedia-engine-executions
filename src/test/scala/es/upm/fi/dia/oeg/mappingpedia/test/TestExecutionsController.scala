package es.upm.fi.dia.oeg.mappingpedia.test

import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.controller.MappingExecutionController
import es.upm.fi.dia.oeg.mappingpedia.model.MappingExecution

object TestExecutionsController {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  val executionsController = MappingExecutionController.apply();

  def main(args:Array[String]) = {
    this.testExecuteMapping1();
    //this.testExecuteMapping2();
    //this.testQueryMapping();
    //this.testFindInstancesByClassName("Building");
    //this.testQueryMapping2();
  }

  def testExecuteMapping1() {
    val organizationId = "test-mobileage-upm4";
    val ckanPackageId = "0f451a73-c216-4b6d-ae26-2376ecd1153d";
    val datasetId = "5f0ac042-a289-4a5a-a1ad-a0a760cfbcf9"
    val distributionDownloadUrl = List("https://raw.githubusercontent.com/oeg-upm/mappingpedia-engine/master/examples/edificio-historico.csv");
    val mdDownloadUrl = "https://raw.githubusercontent.com/oeg-upm/mappingpedia-engine/master/examples/edificio-historico.r2rml.ttl";

    val mappingExecution = MappingExecution(
      organizationId
      , ckanPackageId
      , distributionDownloadUrl
      , mdDownloadUrl
    );

    val result = executionsController.executeMapping(mappingExecution);
    val resultDownloadUrl = result.getMapping_execution_result_download_url;
    logger.info(s"resultDownloadUrl = ${resultDownloadUrl}")
  }

  def testExecuteMapping2() {
    val datasetId = "5f0ac042-a289-4a5a-a1ad-a0a760cfbcf9"
    val mdId = "0cc9daaa-b425-40e9-a74c-e5ea88a17cc1"

    val mappingExecution = MappingExecution(datasetId, mdId, null);

    val result = executionsController.executeMapping(mappingExecution);
    val resultDownloadUrl = result.getMapping_execution_result_download_url;
    logger.info(s"resultDownloadUrl = ${resultDownloadUrl}")
  }


  def testFindInstancesByClassName(className:String) = {
    val instances = executionsController.findInstancesByClass(className)
    logger.info(s"instances = ${instances}")
  }

  def testQueryMapping1() {
    val datasetId = "5f0ac042-a289-4a5a-a1ad-a0a760cfbcf9"
    val mdId= "0cc9daaa-b425-40e9-a74c-e5ea88a17cc1";
    val queryUrl = "https://raw.githubusercontent.com/oeg-upm/morph-rdb/master/morph-examples/examples-csv/edificio-historico-q1.rq";

    val mappingExecution = MappingExecution(datasetId, mdId, queryUrl);

    val result = executionsController.executeMapping(mappingExecution);
    val resultDownloadUrl = result.getMapping_execution_result_download_url;
    logger.info(s"resultDownloadUrl = ${resultDownloadUrl}")
  }

  def testQueryMapping2() {
    val datasetId = "f0eb922b-122b-49e9-a4d2-fa0eb8b592d6"
    val mdId= "300bea7f-0a5d-4962-bb77-64193c571d49";
    val queryUrl = "https://raw.githubusercontent.com/oeg-upm/morph-rdb/master/morph-examples/examples-csv/preview.rq";

    val mappingExecution = MappingExecution(datasetId, mdId, queryUrl);

    val result = executionsController.executeMapping(mappingExecution);
    val resultDownloadUrl = result.getMapping_execution_result_download_url;
    logger.info(s"resultDownloadUrl = ${resultDownloadUrl}")
  }
}