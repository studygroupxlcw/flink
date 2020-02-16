package jwr;

public class WordCount {
    private String word;
    private Integer count;
    private long timestamp;

    public WordCount() {
    }

    public WordCount(String word, Integer count,Long timestamp) {
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

    @Override
    public String toString() {
        return "WordCount{" +
                "filename='" + word.split(":")[0] + '\'' +
                ", word=" + word.split(":")[1] +
                ", count=" + count +
                ", timestamp=" + timestamp +
                '}';
    }
}
