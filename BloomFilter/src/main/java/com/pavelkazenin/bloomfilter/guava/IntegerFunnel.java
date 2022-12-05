package com.pavelkazenin.bloomfilter.guava;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public enum IntegerFunnel implements Funnel<Integer> {
    INSTANCE;
    public void funnel(Integer murmurHash3Code, PrimitiveSink into) {
        into.putInt(murmurHash3Code);
    }

}
