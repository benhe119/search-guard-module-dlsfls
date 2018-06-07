/*
 * Copyright 2016 by floragunn UG (haftungsbeschränkt) - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.searchguard.configuration;

//This implementation is based on
//https://github.com/apache/lucene-solr/blob/branch_6_3/lucene/test-framework/src/java/org/apache/lucene/index/FieldFilterLeafReader.java
//https://github.com/apache/lucene-solr/blob/branch_6_3/lucene/misc/src/java/org/apache/lucene/index/PKIndexSplitter.java
//https://github.com/salyh/elasticsearch-security-plugin/blob/4b53974a43b270ae77ebe79d635e2484230c9d01/src/main/java/org/elasticsearch/plugins/security/filter/DlsWriteFilter.java

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

class DlsFlsFilterLeafReader extends FilterLeafReader {

    private static final String[] EMPTY_STRING_ARRAY = new String[0];
    private final Set<String> includesSet;
    private final Set<String> excludesSet;
    private final FieldInfos flsFieldInfos;
    private Bits liveDocs;
    private int numDocs;
    private final boolean flsEnabled;
    private final boolean dlsEnabled;
    private String[] includes;
    private String[] excludes;
    private boolean canOptimize = true;
    private Function<Map<String, ?>, Map<String, Object>> filterFunction;

    DlsFlsFilterLeafReader(final LeafReader delegate, final Set<String> includesExcludes, final BitSetProducer bsp) {
        super(delegate);
        flsEnabled = includesExcludes != null && !includesExcludes.isEmpty();
        dlsEnabled = bsp != null;
        
        if (flsEnabled) {

            final FieldInfos infos = delegate.getFieldInfos();
            this.includesSet = new HashSet<String>(includesExcludes.size());
            this.excludesSet = new HashSet<String>(includesExcludes.size());

            for (final String incExc : includesExcludes) {
                if (canOptimize && (incExc.indexOf('.') > -1 || incExc.indexOf('*') > -1)) {
                    canOptimize = false;
                }

                final char firstChar = incExc.charAt(0);
                
                if (firstChar == '!' || firstChar == '~') {
                    excludesSet.add(incExc.substring(1));
                } else {
                    includesSet.add(incExc);
                }
            }

            int i = 0;
            final FieldInfo[] fa = new FieldInfo[infos.size()];

            if (canOptimize) {
                if (!excludesSet.isEmpty()) {
                    for (final FieldInfo info : infos) {
                        if (!excludesSet.contains(info.name)) {
                            fa[i++] = info;
                        }
                    }
                } else {
                    for (final String inc : includesSet) {
                        FieldInfo f;
                        if ((f = infos.fieldInfo(inc)) != null) {
                            fa[i++] = f;
                        }
                    }
                }
            } else {
                if (!excludesSet.isEmpty()) {
                    for (final FieldInfo info : infos) {
                        if (!WildcardMatcher.matchAny(excludesSet, info.name)) {
                            fa[i++] = info;
                        }
                    }

                    this.excludes = excludesSet.toArray(EMPTY_STRING_ARRAY);

                } else {
                    for (final FieldInfo info : infos) {
                        if (WildcardMatcher.matchAny(includesSet, info.name)) {
                            fa[i++] = info;
                        }
                    }

                    this.includes = includesSet.toArray(EMPTY_STRING_ARRAY);
                }
                
                if (!excludesSet.isEmpty()) {
                    filterFunction = XContentMapValues.filter(null, excludes);
                } else {
                    filterFunction = XContentMapValues.filter(includes, null);
                }
            }

            final FieldInfo[] tmp = new FieldInfo[i];
            System.arraycopy(fa, 0, tmp, 0, i);
            this.flsFieldInfos = new FieldInfos(tmp);
            
            
            
        } else {
            this.includesSet = null;
            this.excludesSet = null;
            this.flsFieldInfos = null;
        }
        
        
        if(dlsEnabled) {
            try {
                
                
                
                /*
                  
                Use index cache to do this in a cached way (leverage bitsetproducer)
                
                 
                //borrowed from Apache Lucene (Copyright Apache Software Foundation (ASF))
                final IndexSearcher searcher = new IndexSearcher(this);
                searcher.setQueryCache(null);
                final boolean needsScores = false;
                final Weight preserveWeight = searcher.createNormalizedWeight(dlsQuery, needsScores);
                
                final int maxDoc = in.maxDoc();
                final FixedBitSet bits = new FixedBitSet(maxDoc);
                final Scorer preverveScorer = preserveWeight.scorer(this.getContext());
                if (preverveScorer != null) {
                  bits.or(preverveScorer.iterator());
                }

                if (in.hasDeletions()) {
                    final Bits oldLiveDocs = in.getLiveDocs();
                    assert oldLiveDocs != null;
                    final DocIdSetIterator it = new BitSetIterator(bits, 0L);
                    for (int i = it.nextDoc(); i != DocIdSetIterator.NO_MORE_DOCS; i = it.nextDoc()) {
                      if (!oldLiveDocs.get(i)) {
                        bits.clear(i);
                      }
                    }
                  }

                  this.liveDocs = bits;
                  this.numDocs = bits.cardinality();
                  */
                
            	//get the possibly cached bitset for the dls query
                final BitSet bs = bsp.getBitSet(this.getContext()); //we use 'this.' instead of 'in.' to keep fls effective

                if(bs == null) { //no documents match
                    this.liveDocs = new Bits.MatchNoBits(in.maxDoc());
                    this.numDocs = 0;
                    return;
                }
                
                final Bits currentLiveDocs = in.getLiveDocs();
                
                if(currentLiveDocs == null) { //originally to all documents are live, so we return those matching the dls query
                    this.liveDocs = bs;
                    this.numDocs = bs.cardinality();
                    return;
                }
                
                
                int localNumDocs = 0;
                
                DocIdSetIterator it = new BitSetIterator(bs, 0L);

                //count the non deleted live docs
                for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                    if (currentLiveDocs.get(doc)) {
                        localNumDocs++;
                    }
                }
                
                this.numDocs = localNumDocs;

                //originally not all documents were live (there are deleted ones), so we must 'AND' them with the dls query
                this.liveDocs = new Bits() {

                    @Override
                    public boolean get(int index) {
                        return bs.get(index) && currentLiveDocs.get(index);
                    }

                    @Override
                    public int length() {
                        return bs.length();
                    }
                    
                };
                
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        } else {
            this.liveDocs = null;
            this.numDocs = -1;
        }
    }

    private static class DlsFlsSubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {

        private final Set<String> includes;
        private final BitSetProducer bsp;

        public DlsFlsSubReaderWrapper(final Set<String> includes, final BitSetProducer bsp) {
            this.includes = includes;
            this.bsp = bsp;
        }

        @Override
        public LeafReader wrap(final LeafReader reader) {
            return new DlsFlsFilterLeafReader(reader, includes, bsp);
        }

    }

    static class DlsFlsDirectoryReader extends FilterDirectoryReader {

        private final Set<String> includes;
        private final BitSetProducer bsp;

        public DlsFlsDirectoryReader(final DirectoryReader in, final Set<String> includes, final BitSetProducer bsp) throws IOException {
            super(in, new DlsFlsSubReaderWrapper(includes, bsp));
            this.includes = includes;
            this.bsp = bsp;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(final DirectoryReader in) throws IOException {
            return new DlsFlsDirectoryReader(in, includes, bsp);
        }

        @Override
        public Object getCoreCacheKey() {
            return in.getCoreCacheKey();
        }
    }

    @Override
    public void document(final int docID, final StoredFieldVisitor visitor) throws IOException {
        if(flsEnabled) {
            in.document(docID, new FlsStoredFieldVisitor(visitor));
        } else {
            in.document(docID, visitor);
        }
    }

    private boolean isFls(final String name) {
        
        if(!flsEnabled) {
            return true;
        }
        
        return flsFieldInfos.fieldInfo(name) != null;
    }

    @Override
    public FieldInfos getFieldInfos() {
        
        if(!flsEnabled) {
            return in.getFieldInfos();
        }
        
        return flsFieldInfos;
    }

    @Override
    public Fields fields() throws IOException {
        final Fields fields = in.fields();
        
        if(!flsEnabled) {
            return fields;
        }
        
        return new Fields() {

            @Override
            public Iterator<String> iterator() {
                return Iterators.<String> filter(fields.iterator(), new Predicate<String>() {

                    @Override
                    public boolean apply(final String input) {
                        return isFls(input);
                    }
                });
            }

            @Override
            public Terms terms(final String field) throws IOException {

                if (!isFls(field)) {
                    return null;
                }

                return in.terms(field);

            }

            @Override
            public int size() {
                return flsFieldInfos.size();
            }

        };
    }

    private class FlsStoredFieldVisitor extends StoredFieldVisitor {

        private final StoredFieldVisitor delegate;

        public FlsStoredFieldVisitor(final StoredFieldVisitor delegate) {
            super();
            this.delegate = delegate;
        }

        @Override
        public void binaryField(final FieldInfo fieldInfo, final byte[] value) throws IOException {

            if (fieldInfo.name.equals("_source")) {
                final BytesReference bytesRef = new BytesArray(value);
                final Tuple<XContentType, Map<String, Object>> bytesRefTuple = XContentHelper.convertToMap(bytesRef, false);
                Map<String, Object> filteredSource = bytesRefTuple.v2();
                
                if (!canOptimize) {
                    filteredSource = filterFunction.apply(bytesRefTuple.v2());
                } else {
                    if (!excludesSet.isEmpty()) {
                        filteredSource.keySet().removeAll(excludesSet);
                    } else {
                        filteredSource.keySet().retainAll(includesSet);
                    }
                }
                
                final XContentBuilder xBuilder = XContentBuilder.builder(bytesRefTuple.v1().xContent()).map(filteredSource);
                delegate.binaryField(fieldInfo, BytesReference.toBytes(xBuilder.bytes()));
            } else {
                delegate.binaryField(fieldInfo, value);
            }
        }

        
        @Override
        public Status needsField(final FieldInfo fieldInfo) throws IOException {
            return isFls(fieldInfo.name) ? delegate.needsField(fieldInfo) : Status.NO;
        }

        @Override
        public int hashCode() {
            return delegate.hashCode();
        }

        @Override
        public void stringField(final FieldInfo fieldInfo, final byte[] value) throws IOException {
            delegate.stringField(fieldInfo, value);
        }

        @Override
        public void intField(final FieldInfo fieldInfo, final int value) throws IOException {
            delegate.intField(fieldInfo, value);
        }

        @Override
        public void longField(final FieldInfo fieldInfo, final long value) throws IOException {
            delegate.longField(fieldInfo, value);
        }

        @Override
        public void floatField(final FieldInfo fieldInfo, final float value) throws IOException {
            delegate.floatField(fieldInfo, value);
        }

        @Override
        public void doubleField(final FieldInfo fieldInfo, final double value) throws IOException {
            delegate.doubleField(fieldInfo, value);
        }

        @Override
        public boolean equals(final Object obj) {
            return delegate.equals(obj);
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

    }

    @Override
    public Fields getTermVectors(final int docID) throws IOException {
        final Fields fields = in.getTermVectors(docID);

        if (!flsEnabled || fields == null) {
            return fields;
        }

        return new Fields() {

            @Override
            public Iterator<String> iterator() {
                return Iterators.<String> filter(fields.iterator(), new Predicate<String>() {

                    @Override
                    public boolean apply(final String input) {
                        return isFls(input);
                    }
                });
            }

            @Override
            public Terms terms(final String field) throws IOException {

                if (!isFls(field)) {
                    return null;
                }

                return in.terms(field);

            }

            @Override
            public int size() {
                return flsFieldInfos.size();
            }

        };
    }

    @Override
    public NumericDocValues getNumericDocValues(final String field) throws IOException {
        return isFls(field) ? in.getNumericDocValues(field) : null;
    }

    @Override
    public BinaryDocValues getBinaryDocValues(final String field) throws IOException {
        return isFls(field) ? in.getBinaryDocValues(field) : null;
    }

    @Override
    public SortedDocValues getSortedDocValues(final String field) throws IOException {
        return isFls(field) ? in.getSortedDocValues(field) : null;
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(final String field) throws IOException {
        return isFls(field) ? in.getSortedNumericDocValues(field) : null;
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(final String field) throws IOException {
        return isFls(field) ? in.getSortedSetDocValues(field) : null;
    }

    @Override
    public NumericDocValues getNormValues(final String field) throws IOException {
        return isFls(field) ? in.getNormValues(field) : null;
    }

    @Override
    public Bits getDocsWithField(final String field) throws IOException {
        return isFls(field) ? in.getDocsWithField(field) : null;
    }

    @Override
    public Object getCoreCacheKey() {
        return in.getCoreCacheKey();
    }

    @Override
    public Bits getLiveDocs() {
        
        if(dlsEnabled) {
            return liveDocs;
        }
        
        return in.getLiveDocs();
    }

    @Override
    public int numDocs() {
        
        if(dlsEnabled) {
            return numDocs;
        }
        
        return in.numDocs();
    }

    @Override
    public LeafReader getDelegate() {
        return in;
    }
}
