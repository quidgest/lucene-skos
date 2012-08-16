package at.ac.univie.mminf.luceneSKOS.search;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
  
  private static final boolean enabledAlternatives = false;
  
  private Directory autoCompleteDirectory;
  
  private IndexReader autoCompleteReader;
  
  private IndexSearcher autoCompleteSearcher;
  
  private Set<String> languages;
  
  private SKOSEngine skosEngine;
  
  public SKOSAutocompleter(final Version version, String filenameOrURI,
      String... languages) throws IOException {
    
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
    Analyzer analyzer = new PerFieldAnalyzerWrapper(new EnglishAnalyzer(matchVersion), analyzerPerField);
    
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
      
      String word = prefTerms[0];
      
      // ok index the word
      Document doc = new Document();
      doc.add(new StringField(SOURCE_WORD_FIELD, word, Field.Store.YES)); // orig
                                                                          // term
      doc.add(new TextField(SIMPLE_WORD_FIELD, word, Field.Store.YES)); // tokenized
      doc.add(new TextField(GRAMMED_WORDS_FIELD, word, Field.Store.YES)); // grammed
      
      if (enabledAlternatives ) {
        String[] altTerms = sourceDoc.getValues("alt");
        for (String alt : altTerms) {
          doc.add(new TextField(GRAMMED_WORDS_FIELD, alt, Field.Store.YES)); // grammed
        }
      }
      
      writer.addDocument(doc);
    }
    
    sourceReader.close();
    
    // close writer
    writer.close();
  }
  
  public String[] suggestSimilar(String word, int numSug) throws IOException {
    // get the top 5 terms for query
    StandardQueryParser queryParser = new StandardQueryParser(new EnglishAnalyzer(matchVersion));
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
    List<String> suggestions = new ArrayList<String>();
    for (ScoreDoc doc : docs.scoreDocs) {
      suggestions.add(autoCompleteReader.document(doc.doc).get(
          SOURCE_WORD_FIELD));
    }
    
    return suggestions.toArray(new String[suggestions.size()]);
  }
  
}
