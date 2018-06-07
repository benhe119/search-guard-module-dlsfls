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

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.google.common.collect.Sets;

public class SearchGuardFlsDlsIndexSearcherWrapper extends SearchGuardIndexSearcherWrapper {

    private final static Set<String> metaFields = Sets.union(Sets.newHashSet("_source", "_version"), 
            Sets.newHashSet(MapperService.getAllMetaFields()));
    private final IndexService is;

    public static void printLicenseInfo() {
        System.out.println("***************************************************");
        System.out.println("Searchguard DLS/FLS(+) Security is not free software");
        System.out.println("for commercial use in production.");
        System.out.println("You have to obtain a license if you ");
        System.out.println("use it in production.");
        System.out.println("(+) Document-/Fieldlevel");
        System.out.println("***************************************************");
    }

    static {
        printLicenseInfo();
    }

    public SearchGuardFlsDlsIndexSearcherWrapper(final IndexService indexService, final Settings settings) {
        super(indexService, settings);
        this.is = indexService;
    }

    @Override
    protected DirectoryReader dlsFlsWrap(final DirectoryReader reader) throws IOException {

        Set<String> flsFields = null;
        
        final Map<String, Set<String>> allowedFlsFields = (Map<String, Set<String>>) HeaderHelper.deserializeSafeFromHeader(threadContext,
                ConfigConstants.SG_FLS_FIELDS);
        final Map<String, Set<String>> queries = (Map<String, Set<String>>) HeaderHelper.deserializeSafeFromHeader(threadContext,
                ConfigConstants.SG_DLS_QUERY);

        final String flsEval = evalMap(allowedFlsFields, index.getName());
        final String dlsEval = evalMap(queries, index.getName());

        if (flsEval != null) { 
            flsFields = new HashSet<String>(metaFields);
            flsFields.addAll(allowedFlsFields.get(flsEval));
        }
        
        BitSetProducer bsp = null;
        
        if (dlsEval != null) { 
            final Set<String> unparsedDlsQueries = queries.get(dlsEval);
            if(unparsedDlsQueries != null && !unparsedDlsQueries.isEmpty()) {
                final ShardId shardId = ShardUtils.extractShardId(reader);  
                final BitsetFilterCache bsfc = is.cache().bitsetFilterCache();
                //it also possible to put a 'null' value to newQueryShardContext for the index reader but that will disable some optimizations
                final Query dlsQuery = DlsQueryParser.parse(unparsedDlsQueries, is.newQueryShardContext(shardId.getId(), reader, null), is.xContentRegistry());
                bsp = dlsQuery==null?null:bsfc.getBitSetProducer(dlsQuery);
            }
        }
        
        return new DlsFlsFilterLeafReader.DlsFlsDirectoryReader(reader, flsFields, bsp);
    }
        
        
    @Override
    protected IndexSearcher dlsFlsWrap(final IndexSearcher searcher) throws EngineException {

        if(searcher.getIndexReader().getClass() != DlsFlsFilterLeafReader.DlsFlsDirectoryReader.class) {
            throw new RuntimeException("Unexpected index reader class "+searcher.getIndexReader().getClass());
        }
        
        return searcher;
    }
        
    private String evalMap(final Map<String,Set<String>> map, final String index) {

        if (map == null) {
            return null;
        }

        if (map.get(index) != null) {
            return index;
        } else if (map.get("*") != null) {
            return "*";
        }
        if (map.get("_all") != null) {
            return "_all";
        }

        //regex
        for(final String key: map.keySet()) {
            if(WildcardMatcher.containsWildcard(key) 
                    && WildcardMatcher.match(key, index)) {
                return key;
            }
        }

        return null;
    }
}
