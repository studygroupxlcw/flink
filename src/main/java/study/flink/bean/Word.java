package study.flink.bean;

public class Word {
    public String filename;
    public String word;
    public int count;


    public Word() {

    }

    public Word(String filename, String word, int count) {
        this.filename = filename;
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return filename +":"+ word +":"+ count;
    }
}
