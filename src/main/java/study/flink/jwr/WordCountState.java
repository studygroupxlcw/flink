package jwr;

import java.io.Serializable;
import java.sql.Time;
import java.util.Map;

public class WordCountState implements Serializable {
        private Map<String,WordCount>  wordCount;
        public WordCountState( Map<String,WordCount> wordCount) {
            this.wordCount=wordCount;
        }




    public Map<String, WordCount> getWordCount() {
        return wordCount;
    }

    public void setWordCount(Map<String, WordCount> wordCount) {
        this.wordCount = wordCount;
    }
}
