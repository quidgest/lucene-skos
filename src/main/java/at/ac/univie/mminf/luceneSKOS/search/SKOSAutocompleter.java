package at.ac.univie.mminf.luceneSKOS.search;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spell.LuceneDictionary;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.Version;

public final class SKOSAutocompleter {
  
  private final Version matchVersion;
  
  private static final String GRAMMED_WORDS_FIELD = "words";
  
  private static final String SOURCE_WORD_FIELD = "sourceWord";
  
  private Directory autoCompleteDirectory;
  
  private IndexReader autoCompleteReader;
  
  private IndexSearcher autoCompleteSearcher;
  
  private Set<String> languages;
  
  public SKOSAutocompleter(final Version version, String filenameOrURI,
      String... languages) throws IOException {
    matchVersion = version;
    
    String langSig = "";
    if (languages != null) {
      this.languages = new TreeSet<String>(Arrays.asList(languages));
      langSig = "-" + StringUtils.join(this.languages, ".");
    }
    
    String name = FilenameUtils.getName(filenameOrURI);
    File compldataDir = new File("skoscompldata/" + name + langSig);
    
    if (!compldataDir.isDirectory()) {
      File dir = new File("skosdata/" + name + langSig);
      Directory indexDir = FSDirectory.open(dir);
      this.autoCompleteDirectory = FSDirectory.open(compldataDir);
      reIndex(indexDir, "pref");
    }
    
    this.autoCompleteDirectory = FSDirectory.open(compldataDir);
    
    autoCompleteReader = DirectoryReader.open(autoCompleteDirectory);
    autoCompleteSearcher = new IndexSearcher(autoCompleteReader);
  }
  
  public void reIndex(Directory sourceDirectory, String fieldToAutocomplete)
      throws CorruptIndexException, IOException {
    // build a dictionary (from the spell package)
    IndexReader sourceReader = DirectoryReader.open(sourceDirectory);
    
    LuceneDictionary dict = new LuceneDictionary(sourceReader,
        fieldToAutocomplete);
    
    Analyzer analyzer = new Analyzer() {
      
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
    
    BytesRefIterator iter = dict.getWordsIterator();
    BytesRef bytesRef;
    while ((bytesRef = iter.next()) != null) {
      String word = bytesRef.utf8ToString();
      
      int len = word.length();
      if (len < 3) {
        continue; // too short we bail but "too long" is fine...
      }
    
      // ok index the word
      Document doc = new Document();
      doc.add(new StringField(SOURCE_WORD_FIELD, word, Field.Store.YES)); // orig
                                                                          // term
      doc.add(new TextField(GRAMMED_WORDS_FIELD, word, Field.Store.YES)); // grammed
      writer.addDocument(doc);
    }
    
    sourceReader.close();
    
    // close writer
    writer.close();
  }
  
  public String[] suggestSimilar(String word, int numSug) throws IOException {
    // get the top 5 terms for query
    Query query = new TermQuery(new Term(GRAMMED_WORDS_FIELD, word));
    
    TopDocs docs = autoCompleteSearcher.search(query, null, 5);
    List<String> suggestions = new ArrayList<String>();
    for (ScoreDoc doc : docs.scoreDocs) {
      suggestions.add(autoCompleteReader.document(doc.doc).get(
          SOURCE_WORD_FIELD));
    }
    
    return suggestions.toArray(new String[suggestions.size()]);
  }
  
}
