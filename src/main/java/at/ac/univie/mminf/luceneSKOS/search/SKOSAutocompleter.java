package at.ac.univie.mminf.luceneSKOS.search;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter.Side;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import at.ac.univie.mminf.luceneSKOS.skos.SKOSEngine;
import at.ac.univie.mminf.luceneSKOS.skos.impl.SKOSEngineImpl;

public final class SKOSAutocompleter {
  
  private final Version matchVersion;
  
  private static final String GRAMMED_WORDS_FIELD = "words";
  
  private static final String SIMPLE_WORD_FIELD = "simpleWord";
  
  private static final String SOURCE_WORD_FIELD = "sourceWord";
  
  private static final boolean enabledAlternatives = true;
  
  private Directory autoCompleteDirectory;
  
  private IndexReader autoCompleteReader;
  
  private IndexSearcher autoCompleteSearcher;
  
  private Set<String> languages;
  
  private SKOSEngine skosEngine;
  
  public SKOSAutocompleter(final Version version, String filenameOrURI,
      String... languages) throws IOException {
    
    // Makes sure we have a source!
    skosEngine = new SKOSEngineImpl(version, filenameOrURI, languages);
    
    matchVersion = version;
    
    String langSig = "";
    if (languages != null && languages.length > 0) {
      this.languages = new TreeSet<String>(Arrays.asList(languages));
      langSig = "-" + StringUtils.join(this.languages, ".");
    }
    
    String name = FilenameUtils.getName(filenameOrURI);
    File compldataDir = new File("skoscompldata/" + name + langSig);
    
    if (!compldataDir.isDirectory()) {
      File dir = new File("skosdata/" + name + langSig);
      Directory indexDir = FSDirectory.open(dir);
      this.autoCompleteDirectory = FSDirectory.open(compldataDir);
      reIndex(indexDir);
    }
    
    this.autoCompleteDirectory = FSDirectory.open(compldataDir);
    
    autoCompleteReader = DirectoryReader.open(autoCompleteDirectory);
    autoCompleteSearcher = new IndexSearcher(autoCompleteReader);
  }
  
  public void reIndex(Directory sourceDirectory)
      throws CorruptIndexException, IOException {
    IndexReader sourceReader = DirectoryReader.open(sourceDirectory);
    
    Analyzer analyzerEdge = new Analyzer() {
      
      @Override
      protected TokenStreamComponents createComponents(String fieldName,
          Reader reader) {
        final StandardTokenizer src = new StandardTokenizer(matchVersion,
            reader);
        TokenStream tok = new StandardFilter(matchVersion, src);
        tok = new StandardFilter(matchVersion, tok);
        tok = new LowerCaseFilter(matchVersion, tok);
        tok = new StopFilter(matchVersion, tok,
            EnglishAnalyzer.getDefaultStopSet());
        tok = new EdgeNGramTokenFilter(tok, Side.FRONT, 1, 20);
        return new TokenStreamComponents(src, tok);
      }
    };
    
    Map<String,Analyzer> analyzerPerField = new HashMap<String,Analyzer>();
    analyzerPerField.put(GRAMMED_WORDS_FIELD, analyzerEdge);
    Analyzer analyzer = new PerFieldAnalyzerWrapper(new StandardAnalyzer(matchVersion), analyzerPerField);
    
    LogMergePolicy mp = new LogByteSizeMergePolicy();
    mp.setMergeFactor(300);
    
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(matchVersion,
        analyzer).
        setMaxBufferedDocs(150).
        setMergePolicy(mp).
        setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    
    // use a custom analyzer so we can do EdgeNGramFiltering
    IndexWriter writer = new IndexWriter(autoCompleteDirectory,
        indexWriterConfig);
    
    for (int i=0; i < sourceReader.maxDoc(); i++) {
      Document sourceDoc = sourceReader.document(i);
      
      String[] prefTerms = sourceDoc.getValues("pref");
      if (prefTerms.length == 0)
        continue;
      
      String[] altTerms = sourceDoc.getValues("alt");
      
      for (String pref : prefTerms) {
      
        Document doc = new Document();
        
        // ok index the word
      
        doc.add(new StringField(SOURCE_WORD_FIELD, pref, Field.Store.YES)); // orig term
        doc.add(new TextField(GRAMMED_WORDS_FIELD, pref, Field.Store.YES)); // grammed
        
        // add other languages syns
        for (String pref1 : prefTerms) {
          doc.add(new TextField(SIMPLE_WORD_FIELD, pref1, Field.Store.YES)); // tokenized
        }
        
        if (enabledAlternatives ) {
          for (String alt : altTerms) {
            doc.add(new TextField(SIMPLE_WORD_FIELD, alt, Field.Store.YES)); // tokenized
          }
        }
        
        writer.addDocument(doc);
      }
    }
    
    sourceReader.close();
    
    // close writer
    writer.close();
  }
  
  public String[] suggestSimilar(String word, int numSug) throws IOException {
    // get the top terms for query
    StandardQueryParser queryParser = new StandardQueryParser(new StandardAnalyzer(matchVersion));
    queryParser.setDefaultOperator(Operator.AND);
    Query queryExact;
    Query queryLax;
    try {
      // TODO: Use seperate field for this
      queryExact = queryParser.parse(word, SIMPLE_WORD_FIELD);
      queryLax = queryParser.parse(word, GRAMMED_WORDS_FIELD);
      // Query query = new TermQuery(new Term(GRAMMED_WORDS_FIELD, word));
    } catch (QueryNodeException e) {
      return new String[0];
    }
    
    queryExact.setBoost(5);
    
    BooleanQuery query = new BooleanQuery();
    query.add(queryExact, Occur.SHOULD);
    query.add(queryLax, Occur.SHOULD);
    
    TopDocs docs = autoCompleteSearcher.search(query, null, numSug);
    int hits = docs.totalHits;
    
    LinkedHashSet<String> suggestions = new LinkedHashSet<String>();
    for (ScoreDoc doc : docs.scoreDocs) {
      Document d = autoCompleteReader.document(doc.doc);
      String sourceWord = d.get(SOURCE_WORD_FIELD);
      suggestions.add(sourceWord);
    }
    
    // Add count to the end of the list
    suggestions.add(String.valueOf(hits));
    
    return suggestions.toArray(new String[suggestions.size()]);
  }
  
}
