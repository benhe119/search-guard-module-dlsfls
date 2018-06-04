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

package com.floragunn.searchguard.dlic.dlsfls;

import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class DlsMultipleShardsTest extends AbstractDlsFlsTest{
    
    
    protected void populate(TransportClient tc) {

        tc.index(new IndexRequest("searchguard").type("config").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("config", FileHelper.readYamlContent("sg_config.yml"))).actionGet();
        tc.index(new IndexRequest("searchguard").type("internalusers").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("0")
                .source("internalusers", FileHelper.readYamlContent("sg_internal_users.yml"))).actionGet();
        tc.index(new IndexRequest("searchguard").type("roles").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("roles", FileHelper.readYamlContent("sg_roles_shards.yml"))).actionGet();
        tc.index(new IndexRequest("searchguard").type("rolesmapping").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("0")
                .source("rolesmapping", FileHelper.readYamlContent("sg_roles_mapping.yml"))).actionGet();
        tc.index(new IndexRequest("searchguard").type("actiongroups").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("0")
                .source("actiongroups", FileHelper.readYamlContent("sg_action_groups.yml"))).actionGet();
        
        tc.admin().indices().create(new CreateIndexRequest("deals0",indexSettings())).actionGet();
        tc.admin().indices().create(new CreateIndexRequest("deals1",indexSettings())).actionGet();
        //tc.admin().indices().create(new CreateIndexRequest("deals12",indexSettings())).actionGet();
        
        for(int i=0; i<90; i++) {
            tc.index(new IndexRequest("deals0").type("won").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"type_won_num\": "+i+", \"origin\":\"public\"}")).actionGet();
            tc.index(new IndexRequest("deals0").type("lost").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"type_lost_num\": "+i+"}")).actionGet();
            tc.index(new IndexRequest("deals0").type("new").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"type_new_num\": "+i+", \"origin\":\"public\"}")).actionGet();
        }
        
        for(int i=0; i<90; i++) {
            tc.index(new IndexRequest("deals1").type("won").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"type_won_num\": "+i+", \"origin\":\"public\"}")).actionGet();
            tc.index(new IndexRequest("deals1").type("lost").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"type_lost_num\": "+i+"}")).actionGet();
            tc.index(new IndexRequest("deals1").type("undecided").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"type_undecided_num\": "+i+", \"origin\":\"public\"}")).actionGet();
        }
        
        //tc.index(new IndexRequest("deals1").type("dealstype2").id("4").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
        //        .source("{\"amount\": 1500}")).actionGet();
        //tc.delete(new DeleteRequest("deals1", "dealstype2", "4").setRefreshPolicy(RefreshPolicy.IMMEDIATE)).actionGet();
       
        
        
    }
    
    public Settings indexSettings() {
        Settings.Builder builder =  Settings.builder()
            .put("number_of_shards", 10)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), true)
            .put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true)
                ;

        return builder.build();
    }
    
    
    
    @Test
    public void testDlsMultipleIndices() throws Exception {
        
        setup();
        
        HttpResponse res;
        
        for(int i=0; i<5;i++) {
            
            Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals0/_search?q=origin:public&pretty&size=1000", new BasicHeader("Authorization", "Basic "+encodeBasicHeader("dept_manager", "password")))).getStatusCode());
            Assert.assertFalse(res.getBody(),res.getBody().contains("deals1"));
            Assert.assertFalse(res.getBody(),res.getBody().contains("\"_type\" : \"new\""));
            Assert.assertFalse(res.getBody(),res.getBody().contains("\"_type\" : \"lost\""));
            Assert.assertFalse(res.getBody(),res.getBody().contains("\"_type\" : \"undecided\""));
            Assert.assertTrue(res.getBody(),res.getBody().contains("\"total\" : 90,\n    \"max_"));
            Assert.assertTrue(res.getBody(),res.getBody().contains("\"failed\" : 0"));

            
            Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals1/_search?q=origin:public&pretty&size=1000", new BasicHeader("Authorization", "Basic "+encodeBasicHeader("dept_manager", "password")))).getStatusCode());
            Assert.assertFalse(res.getBody(),res.getBody().contains("deals0"));
            Assert.assertFalse(res.getBody(),res.getBody().contains("\"_type\" : \"new\""));
            Assert.assertFalse(res.getBody(),res.getBody().contains("\"_type\" : \"lost\""));
            Assert.assertTrue(res.getBody(),res.getBody().contains("\"_type\" : \"undecided\""));
            Assert.assertTrue(res.getBody(),res.getBody().contains("\"total\" : 180,\n    \"max_"));
            Assert.assertTrue(res.getBody(),res.getBody().contains("\"failed\" : 0"));
            
            Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals*/_search?q=origin:public&pretty&size=1000", new BasicHeader("Authorization", "Basic "+encodeBasicHeader("dept_manager", "password")))).getStatusCode());
            Assert.assertFalse(res.getBody(),res.getBody().contains("\"_type\" : \"new\""));
            Assert.assertFalse(res.getBody(),res.getBody().contains("\"_type\" : \"lost\""));
            Assert.assertTrue(res.getBody(),res.getBody().contains("\"_type\" : \"undecided\""));
            Assert.assertTrue(res.getBody(),res.getBody().contains("\"total\" : 270,\n    \"max_"));
            Assert.assertTrue(res.getBody(),res.getBody().contains("\"failed\" : 0"));
        
        }
    }
}