/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.RealtimeRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.support.WildcardMatcher;

public class DlsFlsValveImpl implements DlsFlsRequestValve {

    /**
     * 
     * @param request
     * @param listener
     * @return false on error
     */
    public boolean invoke(final ActionRequest request, final ActionListener<?> listener, ThreadContext threadContext) {
        final Map<String,Set<String>> allowedFlsFields = (Map<String,Set<String>>) HeaderHelper.deserializeSafeFromHeader(threadContext, ConfigConstants.SG_FLS_FIELDS_HEADER);
        final Map<String,Set<String>> queries = (Map<String,Set<String>>) HeaderHelper.deserializeSafeFromHeader(threadContext, ConfigConstants.SG_DLS_QUERY_HEADER);     

        if (allowedFlsFields != null && !allowedFlsFields.isEmpty()) {
            
            String ri = HeaderHelper.getSafeFromHeader(threadContext, "_sg_fls_resolved_indices_cur");
            if(ri == null) {
                ri = HeaderHelper.getSafeFromHeader(threadContext, "_sg_fls_resolved_indices");
            }
            
            if (ri != null) {
                for (Iterator<Entry<String, Set<String>>> it = allowedFlsFields.entrySet().iterator(); it.hasNext();) {
                    Entry<String, Set<String>> entry = it.next();
                    if (!WildcardMatcher.matchAny(entry.getKey(), ri.split(","), false)) {
                        it.remove();
                    }
                }
            }
        }
        
        
        if(allowedFlsFields != null && !allowedFlsFields.isEmpty()) {
            
            if(request instanceof RealtimeRequest) {
                ((RealtimeRequest) request).realtime(Boolean.FALSE);
            }
            
            if(request instanceof UpdateRequest) {
                listener.onFailure(new ElasticsearchSecurityException("Update is not supported when FLS is activated"));
                return false;
            }
            
            if(request instanceof BulkRequest) {
                for(DocWriteRequest<?> inner:((BulkRequest) request).requests()) {
                    if(inner instanceof UpdateRequest) {
                        listener.onFailure(new ElasticsearchSecurityException("Update is not supported when FLS is activated"));
                        return false;
                    }
                }
            }
            
            if(request instanceof SearchRequest) {
                ((SearchRequest)request).requestCache(Boolean.FALSE);
            }
            
        }
        
        if(queries != null && !queries.isEmpty()) {
            if(request instanceof RealtimeRequest) {
                ((RealtimeRequest) request).realtime(Boolean.FALSE);
            }
            
            if(request instanceof SearchRequest) {
                ((SearchRequest)request).requestCache(Boolean.FALSE);
            }
        }
        
        return true;
    }
    
}
