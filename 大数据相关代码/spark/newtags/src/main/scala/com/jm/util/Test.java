package com.jm.util;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;

public class Test {
    public static void main(String[] args) {
        System.out.println(HanLP.segment("今天天气好好"));;
    }
}

