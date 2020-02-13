package jwr;

import java.io.Serializable;
import java.sql.Time;

public class WordCountState implements Serializable {
        private String word;
        private Integer count;
        private long timestamp;
        public WordCountState(String word, Integer count,Long timestamp) {
            this.word = word;
            this.count = count;
            this.timestamp=timestamp;
        }
    public String getWord() {
        return word;
    }
    public long getTimestamp() {return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    public void setWord(String word) {
            this.word = word;
        }
    public Integer getCount() {
            return count;
        }
    public void setCount(Integer count) {
            this.count = count;
        }

}
