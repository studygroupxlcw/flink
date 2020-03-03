package study.flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import study.flink.bean.Word;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FwcAggregator implements AggregateFunction<Word, Map<String, Word>, Set<Word>> {


    @Override
    public Map<String, Word> createAccumulator() {
        System.out.println("[createAcc], hash: "+ this.hashCode());
        return new HashMap<>();
    }

    @Override
    public Map<String, Word> add(Word in, Map<String, Word> acc) {
        Word stock = acc.get(in.word);
        if (stock != null) {
            if (stock.filename.equals(in.filename)) {
                stock.count = stock.count + in.count;
            } else {
                stock.count = 1;
            }
            acc.put(in.word, stock);

        } else {
            acc.put(in.word, in);
        }

        System.out.println("[add], acc size "+ acc.size());
        return acc;
    }

    @Override
    public Set<Word> getResult(Map<String, Word> accumulator) {
        Set<Word> rs = new HashSet<>();
        rs.addAll(accumulator.values());
        System.out.println("[getResult], rs size "+ rs.size());
        return rs;
    }


    @Override
    public Map<String, Word> merge(Map<String, Word> a, Map<String, Word> b) {
        System.out.println("[merge]");
        return null;
    }


}
