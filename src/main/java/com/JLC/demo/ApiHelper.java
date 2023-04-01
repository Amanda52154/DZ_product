package com.JLC.demo;

import okhttp3.*;
import java.io.IOException;

/**
 * DzProduce   com.JLC.demo
 * 2023-03-2023/3/27   11:03
 *
 * @author : zhangmingyue
 * @description : API请求、JSON解析
 * @date : 2023/3/27 11:03 AM
 */
public class ApiHelper {
    private final String apiUrl;

    public ApiHelper(String apiUrl) {
        this.apiUrl = apiUrl;
}
    public String fetchData(String idPath, String categorys) throws IOException {
        String jsonBody = createJsonBody(idPath, categorys);
        return  sendPostRequest(apiUrl, jsonBody);
    }
    public String fetchData01(String jsonBody) throws IOException {
        return  sendPostRequest(apiUrl, jsonBody);
    }

//    Get jsonbody
    private String createJsonBody(String idPath, String categorys) {
        return String.format("{" +
                "\"categorys\": \"%s\"," +
                "\"idPath\": \"%s\"," +
                "\"isPaging\": 1," +
                "\"pageNum\": 1," +
                "\"pageSize\": 1000," +
                "\"subCode\": \"SP,SO,SI,SA,SR,SC,JC,CB,ST,ZJ\"," +
                "\"queryColumns\": \"id,subCode,name,pId,subCode,updFreq,updField,fromDate,toDate,attr,category,namePath,idPath\"" +
                "}", categorys, idPath);
    }
// Request API get data
    private String sendPostRequest(String url,String jsonBody) throws IOException {
        OkHttpClient client = new OkHttpClient();
        RequestBody requestBody = RequestBody.create(jsonBody, MediaType.parse("application/json; charset=utf-8"));
        Request request = new Request.Builder()
                .url(url)
                .post(requestBody)
                .addHeader("Content-Type", "application/json")
                .build();
        Response response = client.newCall(request).execute();

        if (response.body() != null) {
            return response.body().string();
        }
        return null;
    }
}