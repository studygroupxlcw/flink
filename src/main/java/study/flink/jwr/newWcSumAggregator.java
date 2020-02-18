package jwr;

import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;
import java.util.Map;

public class newWcSumAggregator implements AggregateFunction<WordCount, Map<String, WordCount>, Map<String, WordCount>>  {

    @Override
    public Map<String, WordCount> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public Map<String, WordCount> add(WordCount wordCount, Map<String, WordCount> stringWordCountMap) {
        if (stringWordCountMap.containsKey(wordCount.getWord())){
            String filename = stringWordCountMap.get(wordCount.getWord()).getFilename();
            Long times = stringWordCountMap.get(wordCount.getWord()).getTimestamp();
            Integer cnt = stringWordCountMap.get(wordCount.getWord()).getCount();
            if (wordCount.getFilename().equals(filename)&& wordCount.getTimestamp() >= times){
                wordCount.setCount(wordCount.getCount()+cnt);
                wordCount.setTimestamp(wordCount.getTimestamp());
            }else {
                stringWordCountMap.put(wordCount.getWord(),wordCount);
                return stringWordCountMap;
            }
        }else {
            stringWordCountMap.put(wordCount.getWord(),wordCount);
        }

        return stringWordCountMap;
    }

//    @Override
//    public Map<String, WordCount> add(WordCount wordCount, WordCount wordCount2) {
//
//           wordCount2.setTimestamp(wordCount.getTimestamp());
//           wordCount2.setCount(wordCount.getCount());
//           wordCount2.setWord(wordCount.getWord());
//       // System.out.println(wordCount2.getWord()+"++"+wordCount2.getCount());
//        return null;
//    }

    @Override
    public Map<String, WordCount> getResult(Map<String, WordCount> wordCount) {
        return wordCount;
    }

    @Override
    public Map<String, WordCount> merge(Map<String, WordCount> wordCount,Map<String, WordCount> WordCount1 ) {

        return null;
    }
}