package es.upm.fi.dia.oeg.mappingpedia

import java.io.{InputStream, Reader}
import java.util.Properties

import es.upm.fi.dia.oeg.mappingpedia.utility.MappingPediaUtility.logger
import org.slf4j.{Logger, LoggerFactory}


/**
	* Created by freddy on 10/08/2017.
	*/
class MappingPediaProperties(is:InputStream) extends Properties {
	val logger: Logger = LoggerFactory.getLogger(this.getClass);
	super.load(is);

	//VIRTUOSO
	val virtuosoEnabled:Boolean = this.getPropertyAsBoolean("virtuoso.enabled", true)
	val virtuosoJDBC:String = this.getProperty("vjdbc")
	val virtuosoUser:String = this.getProperty("usr")
	val virtuosoPwd:String = this.getProperty("pwd")
	val graphName:String = this.getProperty("graphname")
	val clearGraph = this.getPropertyAsBoolean("cleargraph", false)


	//GITHUB
	val githubEnabled = this.getPropertyAsBoolean("github.enabled", true);
	val githubUser:String = this.getProperty("github.mappingpedia.username")
	val githubAccessToken:String = this.getProperty("github.mappingpedia.accesstoken")
	//val githubRepo = this.getProperty("github.mappingpedia.repository", MappingPediaConstant.DEFAULT_GITHUB_REPOSITORY)
	//val githubRepoContents = this.getProperty("github.mappingpedia.repository.contents", MappingPediaConstant.DEFAULT_GITHUB_REPOSITORY_CONTENTS)

	/*
	val githubUsername = this.getProperty("github.username", "oeg-upm");
	val githubRepository = this.getProperty("github.repository", "mappingpedia-contents");
	*/
	val githubRepository = this.getProperty("github.repository", "oeg-upm/mappingpedia-contents");


	//CKAN
	val ckanEnable = this.getPropertyAsBoolean("ckan.enabled", true);
	val ckanKey:String = this.getProperty("ckan.key")
	/*
    val ckanActionOrganizationCreate:String=this.getProperty("ckan.action.organization.create")
    val ckanActionPackageCreate:String=this.getProperty("ckan.action.package.create")
    val ckanActionResourceCreate:String=this.getProperty("ckan.action.resource.create")
  */

	val ckanURL:String = this.getProperty("ckan.url")
	val ckanActionOrganizationCreate:String=ckanURL + "/api/action/organization_create"
	val ckanActionOrganizationShow:String=ckanURL + "/api/action/organization_show"
	val ckanActionPackageCreate:String=ckanURL + "/api/action/package_create"
  val ckanActionPackageShow:String=ckanURL + "/api/action/package_show"
	val ckanActionPackageUpdate:String=ckanURL + "/api/action/package_update"
	val ckanActionResourceCreate:String=ckanURL + "/api/action/resource_create"
  val ckanActionResourceShow:String=ckanURL + "/api/action/resource_show"


	def getPropertyAsBoolean(propertyKey:String, defaultValue:Boolean) = {
		val propertyValue = this.getProperty(propertyKey);
		if ("true".equalsIgnoreCase(propertyValue) || "yes".equalsIgnoreCase(propertyValue)) { true }
		else if ("false".equalsIgnoreCase(propertyValue) || "no".equalsIgnoreCase(propertyValue)) { false}
		else { defaultValue }
	}
}
