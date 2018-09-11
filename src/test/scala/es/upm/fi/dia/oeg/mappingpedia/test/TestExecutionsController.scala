package es.upm.fi.dia.oeg.mappingpedia.test

import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.controller.MappingExecutionController
import es.upm.fi.dia.oeg.mappingpedia.model.MappingExecution

object TestExecutionsController {
	val logger: Logger = LoggerFactory.getLogger(this.getClass);
	val executionsController = MappingExecutionController.apply();

def main(args:Array[String]) = {
		//this.testExecuteMapping1();
  //this.testExecuteMapping2();
	this.testQueryMapping();
	  //this.testFindInstancesByClassName("Building");
  }

  def testExecuteMapping1() {
    val organizationId = "test-mobileage-upm4";
    val ckanPackageId = "039de928-b279-4182-be18-483f2e5dc031";
		val datasetId = "cde5a55d-302b-4eff-bb15-acd030605e27"
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

  def testQueryMapping() {
		val datasetId = "5f0ac042-a289-4a5a-a1ad-a0a760cfbcf9"
    val mdId= "0cc9daaa-b425-40e9-a74c-e5ea88a17cc1";
    val queryUrl = "https://raw.githubusercontent.com/oeg-upm/morph-rdb/master/morph-examples/examples-csv/edificio-historico-q1.rq";
    
		val mappingExecution = MappingExecution(datasetId, mdId, queryUrl);
		
		val result = executionsController.executeMapping(mappingExecution);
    val resultDownloadUrl = result.getMapping_execution_result_download_url;
    logger.info(s"resultDownloadUrl = ${resultDownloadUrl}")
  }
}