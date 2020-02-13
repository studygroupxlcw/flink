package ptest.f;

import java.util.Arrays;

public class StrTest {
    public static void main(String[] args) {
        String w = "The_Mad_King_-_Edgar_Rice_Burroughs.txt||Barney only shook his head, much to Joseph's evident sorrow.\n";
        String[] words = w.split("\\|\\|")[1].split(" ");
        Arrays.stream(words).forEach(it ->{
//            System.out.println(it);
            System.out.println(it.replaceAll("[^a-zA-Z0-9]", "").toLowerCase());
        });


    }
}
