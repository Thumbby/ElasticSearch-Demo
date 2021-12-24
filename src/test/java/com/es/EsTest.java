package com.es;

import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.ObjectUtils;

import java.io.IOException;
import java.util.*;

/**
 * Created with STEC METADATA DESIGN
 *
 * @author: Chen Lei
 * Date: 2021/12/22 13:49
 * # Time: 13:49
 **/
@SpringBootTest
public class EsTest {

    private RestHighLevelClient client;

    /**
     * 插入数据测试
     * @throws IOException
     */

    @Before
    public void config(){
        this.client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("127.0.0.1", 9200, "http"),
                        new HttpHost("127.0.0.1", 9201, "http"),
                        new HttpHost("127.0.0.1", 9202, "http")
                )
        );
    }
    @Test
    public void contextLoads() throws IOException{

        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("id", 7);
        jsonMap.put("name", "朴秀");
        jsonMap.put("age", 10);
        jsonMap.put("address", "下北泽");
        IndexRequest request = new IndexRequest("test").id("7").source(jsonMap);

        //执行保存
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println("index-------------:"+ indexResponse);
    }

    @Test
    public void query() throws IOException{

        SearchRequest searchRequest = new SearchRequest("test");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //构造查询器(按名字模糊查询)
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("name", "田所浩二")
                .fuzziness(Fuzziness.AUTO);

        searchSourceBuilder.query(matchQueryBuilder);
        //按照id排序
        searchSourceBuilder.sort(new FieldSortBuilder("id").order(SortOrder.DESC));
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        //返回结果分析
        RestStatus status = searchResponse.status();
        TimeValue took = searchResponse.getTook();
        Boolean terminatedEarly = searchResponse.isTerminatedEarly();
        Boolean timedOut = searchResponse.isTimedOut();
        System.out.format("The status:" + status + "\nThe took:" + took + "\nWhether terminated early:"
                            + terminatedEarly + "\nWhether time out:" + timedOut + "\n");

        //统计Shards
        int totalShards = searchResponse.getTotalShards();
        int successfulShards = searchResponse.getSuccessfulShards();
        int failedShards = searchResponse.getFailedShards();
        System.out.println("total shards:" + totalShards + ", successful shards:"+ successfulShards + ", failed shards:" + failedShards);
        for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
            System.out.println(failure);
            // failures should be handled here
        }

        //查询的documents
        SearchHits hits = searchResponse.getHits();
        TotalHits totalHits = hits.getTotalHits();
        // the total number of hits, must be interpreted in the context of totalHits.relation
        long numHits = totalHits.value;
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // do something with the SearchHit
            System.out.println(hit);
        }

    }
}
