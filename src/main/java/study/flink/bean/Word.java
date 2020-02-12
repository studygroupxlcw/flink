package study.flink.bean;

public class Word {

    public String fileName;

    public String value;

    public int cnt;

    public long captureTime;

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Word{");
        sb.append("fileName='").append(fileName).append('\'');
        sb.append(", value='").append(value).append('\'');
        sb.append(", cnt=").append(cnt);
        sb.append(", captureTime=").append(captureTime);
        sb.append('}');
        return sb.toString();
    }
}
