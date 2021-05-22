package com.elasticsearch.example.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.elasticsearch.example.entity.document.MoviesDocument;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;

import static com.elasticsearch.example.util.Constant.MOVIES_INDEX;
import static com.elasticsearch.example.util.Constant.TMDB_INDEX;

@Service
public class ImportService {

    @Autowired
    private RestHighLevelClient client;
    @Autowired
    private ObjectMapper objectMapper;

    public void movies() throws IOException {
        List<MoviesDocument> moviesDocumentList = new ArrayList<>();
        try {
            //(文件完整路径),编码格式
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\Y\\Desktop\\es\\movies.csv"), "utf-8"));//GBK
            String line = null;
            int i = 0;
            while ((line = reader.readLine()) != null) {
                String[] item = line.split(",");//CSV格式文件时候的分割符,我使用的是,号
                String last = item[item.length - 1];//CSV中的数据,如果有标题就不用-1
                System.out.println(last);
                if (i > 0) {
                    getMoviesDocumentList(moviesDocumentList, item);
                }
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        batchInsert(moviesDocumentList);
    }

    /**
     * 单个插入
     *
     * @param item
     * @return
     * @throws IOException
     */
    public String create(String[] item) throws IOException {
        MoviesDocument moviesDocument = new MoviesDocument();
        moviesDocument.setId(item[0]);
        String[] array = item[1].replaceAll("\"", "").split(";");
        if (array.length == 2) {
            moviesDocument.setYear(array[1]);
            moviesDocument.setTitle(array[0] + " " + array[1]);
        } else {
            moviesDocument.setTitle(array[0]);
        }
        moviesDocument.setGenre(Arrays.asList(item[2].split(";")));
        IndexRequest indexRequest = new IndexRequest(MOVIES_INDEX)
                .source(objectMapper.convertValue(moviesDocument, Map.class),
                        XContentType.JSON);
        IndexResponse indexResponse = client.index(indexRequest,
                RequestOptions.DEFAULT);

        return indexResponse.getResult().name();
    }

    public void getMoviesDocumentList(List<MoviesDocument> moviesDocumentList
            , String[] item) {
        MoviesDocument moviesDocument = new MoviesDocument();
        moviesDocument.setId(item[0]);
        String[] array = item[1].replaceAll("\"", "").split(";");
        if (array.length == 2) {
            moviesDocument.setYear(array[1]);
        } else {
            moviesDocument.setYear("2021");
        }
        moviesDocument.setTitle(array[0]);
        moviesDocument.setGenre(Arrays.asList(item[2].split(";")));
        moviesDocumentList.add(moviesDocument);
    }

    /**
     * 批量插入
     *
     * @param moviesDocumentList list
     * @throws IOException Exception
     */
    public void batchInsert(List<MoviesDocument> moviesDocumentList) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        moviesDocumentList.forEach(moviesDocument -> {
            IndexRequest indexRequest = new IndexRequest(MOVIES_INDEX)
                    .source(objectMapper.convertValue(moviesDocument,
                            Map.class),
                            XContentType.JSON);
            bulkRequest.add(indexRequest);
        });
        BulkResponse bulkItemResponses = client.bulk(bulkRequest,
                RequestOptions.DEFAULT);
        // 是否有失败
        System.out.println(bulkItemResponses.hasFailures());
    }


    public void tmdb() {
        String jsonStr = "";
        try {
            File jsonFile = new File("C:\\idea\\2021\\geektime-ELK\\tmdb" +
                    "-search\\tmdb.json");
            FileReader fileReader = new FileReader(jsonFile);

            Reader reader =
                    new InputStreamReader(new FileInputStream(jsonFile), "utf" +
                            "-8");
            int ch = 0;
            StringBuffer sb = new StringBuffer();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            fileReader.close();
            reader.close();
            jsonStr = sb.toString();
            JSONObject jsonObject = JSON.parseObject(jsonStr);

            Map<String, JSONObject> map = new HashMap<>();
            //循环转换
            Iterator it = jsonObject.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, JSONObject> entry =
                        (Map.Entry<String, JSONObject>) it.next();
                map.put(entry.getKey(), entry.getValue());
            }
            BulkRequest bulkRequest = new BulkRequest();
            map.forEach((key, value) -> {
                HashMap<String, JSONObject> map2 = new HashMap<>();
                //循环转换
                Iterator it2 = value.entrySet().iterator();
                while (it2.hasNext()) {
                    Map.Entry<String, JSONObject> entry =
                            (Map.Entry<String, JSONObject>) it2.next();
                    map2.put(entry.getKey(), entry.getValue());
                }
                IndexRequest indexRequest = new IndexRequest(TMDB_INDEX)
                        .source(map2, XContentType.JSON);
                bulkRequest.add(indexRequest);

            });

            BulkResponse bulkItemResponses = client.bulk(bulkRequest,
                    RequestOptions.DEFAULT);
            // 是否有失败
            System.out.println(bulkItemResponses.hasFailures());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}


