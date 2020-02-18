package jwr;

public class WordCount {
    private String filename;
    private String word;
    private Integer count;
    private long timestamp;

    public WordCount() {
    }

    public WordCount(String filename,String word, Integer count,Long timestamp) {
        this.filename=filename;
        this.word = word;
        this.count = count;
        this.timestamp=timestamp;
    }

    public String getFilename() {
        return filename;
    }
    public void setFilename(String filename){
        this.filename = filename;
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

    @Override
    public String toString() {
        return
                "filename='" + filename + '\'' +
                ", word='" + word + '\'' +
                ", count=" + count +
                ", timestamp=" + timestamp ;
    }
}
