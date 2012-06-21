package at.ac.univie.mminf.luceneSKOS.skos.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.FilenameUtils;

import at.ac.univie.mminf.luceneSKOS.skos.SKOS;
import at.ac.univie.mminf.luceneSKOS.skos.SKOSEngine;

import com.hp.hpl.jena.ontology.AnnotationProperty;
import com.hp.hpl.jena.ontology.ObjectProperty;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.util.FileManager;

/**
 * A Jena TDB-backed SKOSEngine Implementation.
 * 
 */
public class TDBSKOSEngineImpl implements SKOSEngine {
  
  /** The input SKOS model */
  private Model skosModel;
  
  /** The input Jena TDB dataset */
  private Dataset dataset;
  
  /**
   * The languages to be considered when returning labels.
   * 
   * If NULL, all languages are supported
   */
  private Set<String> languages;
  
  /**
   * This constructor loads the SKOS model from a given InputStream using the
   * given serialization language parameter, which must be either N3, RDF/XML,
   * or TURTLE.
   * 
   * @param inputStream
   *          the input stream
   * @param lang
   *          the serialization language
   * @throws IOException
   *           if the model cannot be loaded
   */
  public TDBSKOSEngineImpl(InputStream inputStream, String lang)
      throws IOException {
    
    if (!(lang.equals("N3") || lang.equals("RDF/XML") || lang.equals("TURTLE"))) throw new IOException(
        "Invalid RDF serialization format");
    
    skosModel = ModelFactory.createDefaultModel();
    
    skosModel.read(inputStream, null, lang);
  }
  
  /**
   * Constructor for all label-languages
   * 
   * @param filenameOrURI
   *          the name of the skos file to be loaded
   * @throws IOException
   */
  public TDBSKOSEngineImpl(String filenameOrURI) throws IOException {
    this(filenameOrURI, (String[]) null);
  }
  
  /**
   * This constructor loads the SKOS model from a given filename or URI, starts
   * the indexing process and sets up the index searcher.
   * 
   * @param languages
   *          the languages to be considered
   * @param filenameOrURI
   * @throws IOException
   */
  public TDBSKOSEngineImpl(String filenameOrURI, String... languages)
      throws IOException {
    
    if (languages != null) {
      this.languages = new TreeSet<String>(Arrays.asList(languages));
    }
    
    String baseName = FilenameUtils.getBaseName(filenameOrURI);
    File dir = new File("tdbdata/" + baseName);
    
    if (!dir.isDirectory()) {
      dataset = TDBFactory.createDataset(dir.getPath());
      // load the skos model from the given file
      skosModel = dataset.getDefaultModel().add(
          FileManager.get().loadModel(filenameOrURI));
    } else {
      dataset = TDBFactory.createDataset(dir.getPath());
      // just open existing data
      skosModel = dataset.getDefaultModel();
    }
  }
  
  @Override
  public String[] getAltLabels(String conceptURI) throws IOException {
    Resource skos_concept = skosModel.getResource(conceptURI);
    return getAnnotation(skos_concept, SKOS.altLabel);
  }
  
  @Override
  public String[] getAltTerms(String prefLabel) throws IOException {
    List<String> result = new ArrayList<String>();
    
    // convert the query to lower-case
    String queryString = prefLabel.toLowerCase();
    
    try {
      String[] conceptURIs = getConcepts(queryString);
      
      for (String conceptURI : conceptURIs) {
        String[] labels = getAltLabels(conceptURI);
        if (labels != null) for (String label : labels)
          result.add(label);
      }
    } catch (Exception e) {
      System.err
          .println("Error when accessing SKOS Engine.\n" + e.getMessage());
    }
    
    return result.toArray(new String[0]);
  }
  
  @Override
  public String[] getHiddenLabels(String conceptURI) throws IOException {
    Resource skos_concept = skosModel.getResource(conceptURI);
    return getAnnotation(skos_concept, SKOS.hiddenLabel);
  }
  
  @Override
  public String[] getBroaderConcepts(String conceptURI) throws IOException {
    Resource skos_concept = skosModel.getResource(conceptURI);
    return getObject(skos_concept, SKOS.broader);
  }
  
  @Override
  public String[] getBroaderLabels(String conceptURI) throws IOException {
    return getLabels(conceptURI, SKOS.broader);
  }
  
  @Override
  public String[] getBroaderTransitiveConcepts(String conceptURI)
      throws IOException {
    Resource skos_concept = skosModel.getResource(conceptURI);
    return getObject(skos_concept, SKOS.broaderTransitive);
  }
  
  @Override
  public String[] getBroaderTransitiveLabels(String conceptURI)
      throws IOException {
    return getLabels(conceptURI, SKOS.broaderTransitive);
  }
  
  @Override
  public String[] getConcepts(String prefLabel) throws IOException {
    List<String> concepts = new ArrayList<String>();
    
    // convert the query to lower-case
    String queryString = prefLabel.toLowerCase();
    
    getConceptsAux(concepts, queryString, SKOS.prefLabel);
    getConceptsAux(concepts, queryString, SKOS.altLabel);
    getConceptsAux(concepts, queryString, SKOS.hiddenLabel);
    
    return concepts.toArray(new String[0]);
  }
  
  private void getConceptsAux(List<String> concepts, String queryString,
      AnnotationProperty property) {
    ResIterator res_iter = skosModel.listSubjectsWithProperty(property,
        queryString);
    while (res_iter.hasNext()) {
      RDFNode concept = res_iter.next();
      Resource bc = concept.as(Resource.class);
      String uri = bc.getURI();
      concepts.add(uri);
    }
  }
  
  private String[] getLabels(String conceptURI, ObjectProperty property)
      throws IOException {
    List<String> labels = new ArrayList<String>();
    Resource skos_concept = skosModel.getResource(conceptURI);
    String[] concepts = getObject(skos_concept, property);
    
    for (String aConceptURI : concepts) {
      String[] prefLabels = getPrefLabels(aConceptURI);
      labels.addAll(Arrays.asList(prefLabels));
      
      String[] altLabels = getAltLabels(aConceptURI);
      labels.addAll(Arrays.asList(altLabels));
    }
    
    return labels.toArray(new String[0]);
  }
  
  @Override
  public String[] getNarrowerConcepts(String conceptURI) throws IOException {
    Resource skos_concept = skosModel.getResource(conceptURI);
    return getObject(skos_concept, SKOS.narrower);
  }
  
  @Override
  public String[] getNarrowerLabels(String conceptURI) throws IOException {
    return getLabels(conceptURI, SKOS.narrower);
  }
  
  @Override
  public String[] getNarrowerTransitiveConcepts(String conceptURI)
      throws IOException {
    Resource skos_concept = skosModel.getResource(conceptURI);
    return getObject(skos_concept, SKOS.narrowerTransitive);
  }
  
  @Override
  public String[] getNarrowerTransitiveLabels(String conceptURI)
      throws IOException {
    return getLabels(conceptURI, SKOS.narrowerTransitive);
  }
  
  @Override
  public String[] getPrefLabels(String conceptURI) throws IOException {
    Resource skos_concept = skosModel.getResource(conceptURI);
    return getAnnotation(skos_concept, SKOS.prefLabel);
  }
  
  @Override
  public String[] getRelatedConcepts(String conceptURI) throws IOException {
    Resource skos_concept = skosModel.getResource(conceptURI);
    return getObject(skos_concept, SKOS.related);
  }
  
  @Override
  public String[] getRelatedLabels(String conceptURI) throws IOException {
    return getLabels(conceptURI, SKOS.related);
  }
  
  private String[] getAnnotation(Resource skos_concept,
      AnnotationProperty property) {
    List<String> list = new ArrayList<String>();
    StmtIterator stmt_iter = skos_concept.listProperties(property);
    while (stmt_iter.hasNext()) {
      Literal labelLiteral = stmt_iter.nextStatement().getObject()
          .as(Literal.class);
      String label = labelLiteral.getLexicalForm();
      String labelLang = labelLiteral.getLanguage();
      
      if (languages != null && !languages.contains(labelLang)) continue;
      
      // converting label to lower-case
      label = label.toLowerCase();
      
      list.add(label);
    }
    
    return list.toArray(new String[0]);
  }
  
  private String[] getObject(Resource skos_concept, ObjectProperty property) {
    List<String> list = new ArrayList<String>();
    StmtIterator stmt_iter = skos_concept.listProperties(property);
    while (stmt_iter.hasNext()) {
      RDFNode concept = stmt_iter.nextStatement().getObject();
      
      if (!concept.canAs(Resource.class)) {
        System.err.println("Error when indexing relationship of concept "
            + skos_concept.getURI() + " .");
        continue;
      }
      
      Resource bc = concept.as(Resource.class);
      String uri = bc.getURI();
      
      list.add(uri);
    }
    
    return list.toArray(new String[0]);
  }
  
}
