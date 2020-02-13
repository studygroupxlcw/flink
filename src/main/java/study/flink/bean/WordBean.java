package study.flink.bean;

public class WordBean {
    private String fileName;
    private String wordValue;
    private int count;
    private long currentTime;

    public WordBean(String fileName, String wordValue, int count) {
        this.fileName = fileName;
        this.wordValue = wordValue;
        this.count = count;
        currentTime = System.currentTimeMillis() / 1000;
        currentTime -= currentTime % 60;
    }

    public void addCount(int inc) {
        this.count += inc;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getWordValue() {
        return wordValue;
    }

    public void setWordValue(String wordValue) {
        this.wordValue = wordValue;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getCurrentTime() {
        return this.currentTime;
    }

    public void setCurrentTime(long currentTime) {
        this.currentTime = currentTime;
    }

    @Override
    public String toString() {
        return "WordBean{" +
                "fileName='" + fileName + '\'' +
                ", wordValue='" + wordValue + '\'' +
                ", count=" + count +
                '}';
    }
}
