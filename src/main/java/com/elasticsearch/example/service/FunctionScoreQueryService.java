package com.elasticsearch.example.service;

import com.elasticsearch.example.util.ESUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.functionscore.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.elasticsearch.example.util.Constant.BLOGS_INDEX;

@Service
public class FunctionScoreQueryService {
    @Autowired
    private RestHighLevelClient client;

    /**
     * 当 boost > 1 时, 打分的权重相对性提升
     * 当 0 < boost < 1 时, 打分的权重相对性降低
     *
     * @return
     * @throws IOException
     */
    public List<Map> boost() throws IOException {
        SearchRequest searchRequest = new SearchRequest(BLOGS_INDEX);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(new MatchQueryBuilder("content", "rabbits").boost(2f));
        boolQueryBuilder.should(new MatchQueryBuilder("title", "brown").boost(0.1f));

        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest,
                RequestOptions.DEFAULT);
        return ESUtils.buildListMapResp(searchResponse);
    }

    /**
     * positive
     * 必须匹配的条件
     * negative
     * 匹配该条件的文档要减低相关度分数, 最终分数为匹配positive条件的分数 * negative_boost
     * negative_boost
     * 降低匹配negative条件相关度分数的系数, 取值范围为0~1.0
     *
     * @return
     * @throws IOException
     */
    public List<Map> boosting() throws IOException {
        SearchRequest searchRequest = new SearchRequest(BLOGS_INDEX);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("title",
                "rabbits");
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("post_date",
                "2020-01-02");
        BoostingQueryBuilder boostingQueryBuilder =
                new BoostingQueryBuilder(matchQueryBuilder, termQueryBuilder);
        boostingQueryBuilder.negativeBoost(0.2f);


        searchSourceBuilder.query(boostingQueryBuilder);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest,
                RequestOptions.DEFAULT);
        return ESUtils.buildListMapResp(searchResponse);
    }


    /**
     * Function Score Query
     * 提供了几种默认的计算分值的函数
     *  Weight :为每一个文档设置一个简单而不被规范化的权重
     *  Field Value Factor:使用该数值来修改_score, 例如将“热度”和“点赞数”作为算分的参考因素
     *  Random Score:为每一个用户使用一个不同的，随机算分结果
     *  衰减函数:以某个字段的值为标准，距离某个值越近，得分越高
     *  Script Score:自定义脚本完全控制所需逻辑
     * @return
     * @throws IOException
     */
    public List<Map> functionScore() throws IOException {
        SearchRequest searchRequest = new SearchRequest(BLOGS_INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //function1
        RandomScoreFunctionBuilder randomScoreFunctionBuilder =
                ScoreFunctionBuilders
                        .randomFunction()
                        .seed(10) // 同一个seed 随机计算的结果是一样
                        .setField("_seq_no")
                        .setWeight(23);//为函数算分设置一个权重.
        TermQueryBuilder termQueryBuilder1 = new TermQueryBuilder("post_date"
                , "2020-01-01");
        FunctionScoreQueryBuilder.FilterFunctionBuilder filterFunctionBuilder = new FunctionScoreQueryBuilder.FilterFunctionBuilder(termQueryBuilder1, randomScoreFunctionBuilder);
        //function2
        TermQueryBuilder termQueryBuilder2 = new TermQueryBuilder("author_id"
                , 11402);
        WeightBuilder weightBuilder =
                ScoreFunctionBuilders.weightFactorFunction(42);
        FunctionScoreQueryBuilder.FilterFunctionBuilder filterFunctionBuilder2 = new FunctionScoreQueryBuilder.FilterFunctionBuilder(termQueryBuilder2, weightBuilder);
        //query and functions
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("content"
                , "rabbits");
        FunctionScoreQueryBuilder.FilterFunctionBuilder[] filterFunctionBuilders = new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{filterFunctionBuilder, filterFunctionBuilder2};
        FunctionScoreQueryBuilder functionScoreQueryBuilder =
                new FunctionScoreQueryBuilder(matchQueryBuilder,
                        filterFunctionBuilders);
        //  设置一个基础分数
        functionScoreQueryBuilder.boost(5);
        /**
         * * score_mode 存在多个函数时, 计算最终函数分数的方式.
         *   * multiply 多个函数分数相乘, 默认.
         *   * sum 多个函数分数相加.
         *   * avg 取平均值.
         *   * first 匹配条件的第一个函数分数.
         *   * max 取最大值.
         *   * min 取最小值.
         */
        functionScoreQueryBuilder.scoreMode(FunctionScoreQuery.ScoreMode.MULTIPLY);
        /**
         * * Boost Mode
         *   * Multiply: 算分与函数值的乘积
         *   * Sum: 算分与函数的和
         *   * Min / Max: 算分与函数取最小/最大值
         *   * Replace:使用函数值取代算分
         */
        functionScoreQueryBuilder.boostMode(CombineFunction.MULTIPLY);
        // 最小综合分数, 最终综合分数小于min_score的文档将会被过滤掉.
        functionScoreQueryBuilder.setMinScore(42);
        // 可以将算分控制在一个最大值
        functionScoreQueryBuilder.maxBoost(42);
        searchSourceBuilder.query(functionScoreQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest,
                RequestOptions.DEFAULT);
        return ESUtils.buildListMapResp(searchResponse);
    }

    /**
     * Field Value Factor
     *
     * @return
     * @throws IOException
     */
    public List<Map> fieldValueFactor() throws IOException {
        SearchRequest searchRequest = new SearchRequest(BLOGS_INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("content"
                , "rabbits");
        FieldValueFactorFunctionBuilder fieldValueFactorFunctionBuilder =
                // Field Value Factor:使用该数值来修改_score, 例如将“热度”和“点赞数”作为算分的参考因素
                // 新的算分 = 老的算分 * SQRT(1 + factor * author_id)
                // Modifier 可以平滑处理
                ScoreFunctionBuilders
                        .fieldValueFactorFunction("author_id")
                        .factor(1.2f)
                        .modifier(FieldValueFactorFunction.Modifier.SQRT).missing(1);
        FunctionScoreQueryBuilder functionScoreQueryBuilder =
                new FunctionScoreQueryBuilder(matchQueryBuilder,
                        fieldValueFactorFunctionBuilder);
        searchSourceBuilder.query(functionScoreQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest,
                RequestOptions.DEFAULT);
        return ESUtils.buildListMapResp(searchResponse);
    }

    /**
     * Decay Functions 衰减函数:以某个字段的值为标准，距离某个值越近，得分越高
     * @return
     * @throws IOException
     */
    public List<Map> decayFunction() throws IOException {
        SearchRequest searchRequest = new SearchRequest(BLOGS_INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("content"
                , "rabbits");
        GaussDecayFunctionBuilder gaussDecayFunctionBuilder =
                ScoreFunctionBuilders
                        .gaussDecayFunction("post_date", "2020-01-03", "10d", "5d", 0.5);
        FunctionScoreQueryBuilder functionScoreQueryBuilder =
                new FunctionScoreQueryBuilder(matchQueryBuilder,
                        gaussDecayFunctionBuilder);
        searchSourceBuilder.query(functionScoreQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest,
                RequestOptions.DEFAULT);
        return ESUtils.buildListMapResp(searchResponse);
    }

}
