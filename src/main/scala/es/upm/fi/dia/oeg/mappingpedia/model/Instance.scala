package es.upm.fi.dia.oeg.mappingpedia.model

class Instance (val id:String, val rdfType:String, title:String) {
  def getId = this.id
  def getRdf_type = this.rdfType
  def getTitle = this.title

}
