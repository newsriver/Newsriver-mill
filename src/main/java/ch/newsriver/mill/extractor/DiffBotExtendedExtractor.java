/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.newsriver.mill.extractor;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 *
 * @author eliapalme
 */

//This diffbot is rendering the HTML exectuing JavaScript and extracting discussions

public class DiffBotExtendedExtractor extends DiffBotExtractor {



    private static final String API_PATH = "http://api.diffbot.com/v3/article?discussion=false&token=";
    private static final String API_TOKEN = "a93ce2816c0eb44c61cb7b22d31a546c";



    @Override
    protected String getAPIReguest(String URL) throws UnsupportedEncodingException {

        StringBuilder request = new StringBuilder(API_PATH);
        request.append(API_TOKEN);
        request.append("&url=");
        request.append(URLEncoder.encode(URL, "utf-8"));
        return request.toString();
    }




}
