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

package com.floragunn.searchguard.dlic.dlsfls;

import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class FlsDlsTestAB extends AbstractDlsFlsTest{
    
    
    protected void populate(TransportClient tc) {

        tc.index(new IndexRequest("searchguard").type("sg").id("config").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("config", FileHelper.readYamlContent("sg_config.yml"))).actionGet();
        tc.index(new IndexRequest("searchguard").type("sg").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("internalusers")
                .source("internalusers", FileHelper.readYamlContent("sg_internal_users.yml"))).actionGet();
        tc.index(new IndexRequest("searchguard").type("sg").id("roles").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("roles", FileHelper.readYamlContent("sg_roles.yml"))).actionGet();
        tc.index(new IndexRequest("searchguard").type("sg").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("rolesmapping")
                .source("rolesmapping", FileHelper.readYamlContent("sg_roles_mapping.yml"))).actionGet();
        tc.index(new IndexRequest("searchguard").type("sg").setRefreshPolicy(RefreshPolicy.IMMEDIATE).id("actiongroups")
                .source("actiongroups", FileHelper.readYamlContent("sg_action_groups.yml"))).actionGet();
               
        //aaa
        tc.index(new IndexRequest("aaa").type("aaa").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"f1\": \"f1_a0\", \"f2\": \"f2_a0\", \"f3\": \"f3_a0\", \"f4\": \"f4_a0\",\"type\": \"a\"}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("aaa").type("aaa").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"f1\": \"f1_a1\", \"f2\": \"f2_a1\", \"f3\": \"f3_a1\", \"f4\": \"f4_a1\",\"type\": \"a\"}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("aaa").type("aaa").id("2").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"f1\": \"f1_a2\", \"f2\": \"f2_a2\", \"f3\": \"f3_a2\", \"f4\": \"f4_a2\",\"type\": \"x\"}", XContentType.JSON)).actionGet();
        
        //bbb
        tc.index(new IndexRequest("bbb").type("bbb").id("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"f1\": \"f1_b0\", \"f2\": \"f2_b0\", \"f3\": \"f3_b0\", \"f4\": \"f4_b0\",\"type\": \"b\"}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("bbb").type("bbb").id("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"f1\": \"f1_b1\", \"f2\": \"f2_b1\", \"f3\": \"f3_b1\", \"f4\": \"f4_b1\",\"type\": \"b\"}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("bbb").type("bbb").id("2").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source("{\"f1\": \"f1_b2\", \"f2\": \"f2_b2\", \"f3\": \"f3_b2\", \"f4\": \"f4_b2\",\"type\": \"x\"}", XContentType.JSON)).actionGet();
 
        tc.admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices("aaa","bbb").alias("abalias"))).actionGet();
        
    }
    
    @Test
    public void testDlsFlsAB() throws Exception {
        
        setup();
        
        HttpResponse res;
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/aaa,bbb/_search?pretty", encodeBasicHeader("user_aaa", "password"))).getStatusCode());
        System.out.println(res.getBody());
        Assert.assertTrue(res.getBody().contains("\"total\" : 4,\n    \"max_"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        Assert.assertFalse(res.getBody().contains("\"x\""));
        Assert.assertTrue(res.getBody().contains("f1_a"));
        Assert.assertTrue(res.getBody().contains("f2_a"));
        Assert.assertFalse(res.getBody().contains("f3_a"));
        Assert.assertFalse(res.getBody().contains("f4_a"));    
        Assert.assertTrue(res.getBody().contains("f2_b"));
        Assert.assertTrue(res.getBody().contains("f2_b1"));
        Assert.assertTrue(res.getBody().contains("f3_b"));
        Assert.assertFalse(res.getBody().contains("f1_b"));
        
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/abalias/_search?pretty", encodeBasicHeader("user_aaa", "password"))).getStatusCode());
        System.out.println(res.getBody());
        Assert.assertTrue(res.getBody().contains("\"total\" : 4,\n    \"max_"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        Assert.assertFalse(res.getBody().contains("\"x\""));
        Assert.assertTrue(res.getBody().contains("f1_a"));
        Assert.assertTrue(res.getBody().contains("f2_a"));
        Assert.assertFalse(res.getBody().contains("f3_a"));
        Assert.assertFalse(res.getBody().contains("f4_a"));    
        Assert.assertTrue(res.getBody().contains("f2_b"));
        Assert.assertTrue(res.getBody().contains("f2_b1"));
        Assert.assertTrue(res.getBody().contains("f3_b"));
        Assert.assertFalse(res.getBody().contains("f1_b"));
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/aaa,bbb/_search?pretty", encodeBasicHeader("user_bbb", "password"))).getStatusCode());
        System.out.println(res.getBody());
        Assert.assertTrue(res.getBody().contains("\"total\" : 4,\n    \"max_"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        Assert.assertFalse(res.getBody().contains("\"x\""));
        Assert.assertFalse(res.getBody().contains("f1_a"));
        Assert.assertTrue(res.getBody().contains("f2_a"));
        Assert.assertTrue(res.getBody().contains("f3_a"));
        Assert.assertTrue(res.getBody().contains("f4_a"));    
        Assert.assertTrue(res.getBody().contains("f2_b"));
        Assert.assertTrue(res.getBody().contains("f2_b1"));
        Assert.assertFalse(res.getBody().contains("f3_b"));
        Assert.assertTrue(res.getBody().contains("f1_b"));
        
        
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/abalias/_search?pretty", encodeBasicHeader("user_bbb", "password"))).getStatusCode());
        System.out.println(res.getBody());
        Assert.assertTrue(res.getBody().contains("\"total\" : 4,\n    \"max_"));
        Assert.assertTrue(res.getBody().contains("\"failed\" : 0"));
        Assert.assertFalse(res.getBody().contains("\"x\""));
        Assert.assertFalse(res.getBody().contains("f1_a"));
        Assert.assertTrue(res.getBody().contains("f2_a"));
        Assert.assertTrue(res.getBody().contains("f3_a"));
        Assert.assertTrue(res.getBody().contains("f4_a"));    
        Assert.assertTrue(res.getBody().contains("f2_b"));
        Assert.assertTrue(res.getBody().contains("f2_b1"));
        Assert.assertFalse(res.getBody().contains("f3_b"));
        Assert.assertTrue(res.getBody().contains("f1_b"));
    }
}