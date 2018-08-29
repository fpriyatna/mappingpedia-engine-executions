package es.upm.fi.dia.oeg.mappingpedia.test

import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.controller.MappingExecutionController
import es.upm.fi.dia.oeg.mappingpedia.model.MappingExecution

object TestExecutionsController {
	val logger: Logger = LoggerFactory.getLogger(this.getClass);

def main(args:Array[String]) = {
		val executionsController = MappingExecutionController.apply();
		this.testExecuteMapping(executionsController);
  }

  def testExecuteMapping(executionsController:MappingExecutionController) {
    val organizationId = "test-mobileage-upm3";
    val ckanPackageId = "80171420-9042-4414-b844-9c5a651373b5";
    val distributionDownloadUrl = List("https://raw.githubusercontent.com/oeg-upm/mappingpedia-engine/master/examples/edificio-historico.csv");
    val mdDownloadUrl = "https://raw.githubusercontent.com/oeg-upm/mappingpedia-engine/master/examples/edificio-historico.r2rml.ttl";
    
		val mappingExecution = MappingExecution(
		    organizationId
		    , ckanPackageId
		    , distributionDownloadUrl
		    , mdDownloadUrl
				);
		
		//IN THIS PARTICULAR CASE WE HAVE TO STORE THE EXECUTION RESULT ON CKAN
		executionsController.executeMapping(mappingExecution);   
  }
}