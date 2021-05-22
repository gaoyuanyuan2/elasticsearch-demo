package com.elasticsearch.example.service;


import com.elasticsearch.example.util.ESUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.MultiSearchTemplateRequest;
import org.elasticsearch.script.mustache.MultiSearchTemplateResponse;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.elasticsearch.example.util.Constant.MOVIES_INDEX;

/**
 * script模板查询
 */
@Service
public class ScriptQueryService {

    @Autowired
    private RestHighLevelClient client;

    @Autowired
    private RestClient restClient;

    /**
     * 内联模板
     */
    public List<Map> inlineTemplate() throws IOException {
        SearchTemplateRequest request = new SearchTemplateRequest();
        //指定索引
        request.setRequest(new SearchRequest(MOVIES_INDEX));
        //设置为内联
        request.setScriptType(ScriptType.INLINE);
        //设置脚本
        request.setScript("{" + "  \"query\": { \"match\" : { \"{{field}}\" :" +
                " \"{{value}}\" } },"
                + "  \"size\" : \"{{size}}\"" + "}");
        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("field", "title");
        scriptParams.put("value", "life");
        scriptParams.put("size", 5);
        request.setScriptParams(scriptParams);
        SearchTemplateResponse response = client.searchTemplate(request,
                RequestOptions.DEFAULT);
        return ESUtils.buildListMapResp(response);

    }


    /**
     * 注册模板 <br>
     * 注册模板的客户端要是用低级客户端，高级客户端没有提供注册模板<br>
     * 注册模板建议直接执行命令，不要再java中执行，在java中做注册模板，变更时需要重新发布<br>
     *
     * @return 返回注册模板是否注册成功
     */
    public int registerTemplate() throws IOException {
        // GET _scripts/movies_script
        Request scriptRequest = new Request("POST", "_scripts/movies_script");
        String json = "{" +
                "  \"script\": {" +
                "    \"lang\": \"mustache\"," +
                "    \"source\": {" +
                "      \"query\": { \"match\" : { \"{{field}}\" : " +
                "\"{{value}}\" } }," +
                "      \"size\" : \"{{size}}\"" +
                "    }" +
                "  }" +
                "}";
        scriptRequest.setJsonEntity(json);
        int statusCode = -1;
        Response response = restClient.performRequest(scriptRequest);
        // 响应状态行，可以从中获取状态码
        statusCode = response.getStatusLine().getStatusCode();
        return statusCode;
    }

    /**
     * 执行注册模板
     */
    public List<Map> runRegisterTemplate() throws IOException {
        // 可以通过存储的脚本API预先注册搜索模板。
        // 注意，存储的脚本API在高级REST客户机中还不可用，因此在本例中，我们使用低级REST客户机。
        SearchTemplateRequest request = new SearchTemplateRequest();
        request.setRequest(new SearchRequest("movies"));
        request.setScriptType(ScriptType.STORED);
        request.setScript("movies_script");
        Map<String, Object> params = new HashMap<>();
        params.put("field", "title");
        params.put("value", "life");
        params.put("size", 5);
        request.setScriptParams(params);
        // 给定参数值，模板可以在不执行搜索的情况下呈现:
        request.setSimulate(true);
        request.setExplain(true);
        request.setProfile(true);
        SearchTemplateResponse response = client.searchTemplate(request
                , RequestOptions.DEFAULT);
        SearchTemplateRequest request2 = new SearchTemplateRequest();
        request2.setRequest(new SearchRequest("movies"));
        request2.setScriptType(ScriptType.INLINE);
        request2.setScript(response.getSource().utf8ToString());
        request2.setScriptParams(new HashMap<>());
        response = client.searchTemplate(request2, RequestOptions.DEFAULT);
        return ESUtils.buildListMapResp(response);
    }

    public  List<Map> multiTemplate() throws IOException {
        String[] searchTerms = {"life", "mom", "girl"};
        MultiSearchTemplateRequest multiRequest =
                new MultiSearchTemplateRequest();
        for (String searchTerm : searchTerms) {
            SearchTemplateRequest request = new SearchTemplateRequest();
            request.setRequest(new SearchRequest("movies"));

            request.setScriptType(ScriptType.INLINE);
            request.setScript(
                    "{" +
                            "  \"query\": { \"match\" : { \"{{field}}\" : " +
                            "\"{{value}}\" } }," +
                            "  \"size\" : \"{{size}}\"" +
                            "}");

            Map<String, Object> scriptParams = new HashMap<>();
            scriptParams.put("field", "title");
            scriptParams.put("value", searchTerm);
            scriptParams.put("size", 1);
            request.setScriptParams(scriptParams);
            multiRequest.add(request);
        }
        MultiSearchTemplateResponse multiResponse =
                client.msearchTemplate(multiRequest, RequestOptions.DEFAULT);
        return ESUtils.buildListMapResp(multiResponse);
    }

}
