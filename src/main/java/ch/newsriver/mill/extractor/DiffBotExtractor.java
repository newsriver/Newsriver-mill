/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.newsriver.mill.extractor;


import ch.newsriver.data.content.Article;
import ch.newsriver.data.content.Element;
import ch.newsriver.data.content.ImageElement;
import ch.newsriver.data.html.HTML;
import ch.newsriver.util.http.HttpClientPool;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author eliapalme
 */
public class DiffBotExtractor extends ArticleExtractor {


    private static final Logger logger = LogManager.getLogger(DiffBotExtractor.class);
    private static final String API_BASE_URL_DOMAIN = "api.diffbot.com";
    //norender=true    Is to improve speed
    //discussion=false Is to improve speed
    private static final String API_PATH = "http://api.diffbot.com/v3/article?discussion=false&norender=true&token=";
    private static final String API_TOKEN = "";
    private static final ObjectMapper mapper = new ObjectMapper();
    //private static final String API_TOKEN = "a93ce2816c0eb44c61cb7b22d31a546c";
    private static HttpClient client = null;

    protected String getAPIReguest(String URL) throws UnsupportedEncodingException {

        StringBuilder request = new StringBuilder(API_PATH);
        request.append(API_TOKEN);
        request.append("&url=");
        request.append(URLEncoder.encode(URL, "utf-8"));
        return request.toString();
    }


    public Article extractArticle(HTML html) {

        Article article = new Article();


        //here may normalise imgeId by removing repository prefix, etc
        HttpGet httpget = null;
        try {


            httpget = new HttpGet(this.getAPIReguest(html.getUrl()));
            HttpResponse httpresp = null;
            HttpEntity entity = null;
            JsonNode json = null;


            //Retry 3 times as some time diffBot is a bit unstable
            for (int i = 0; i < 3; i++) {
                httpresp = HttpClientPool.getHttpClientInstance().execute(httpget);
                entity = httpresp.getEntity();
                json = mapper.readTree(entity.getContent());
                if (json.has("errorCode") && json.get("errorCode").asLong() == 500) {
                    EntityUtils.consumeQuietly(entity);
                } else {
                    break;
                }
            }


            if (!json.has("objects")) {
                return null;
            }

            Iterator<JsonNode> objects = json.get("objects").elements();
            if (!objects.hasNext()) {
                return null;
            } else {
                JsonNode articleJson = objects.next();
                if (!articleJson.has("type") || !articleJson.get("type").asText().equalsIgnoreCase("article")) {
                    return null;
                }

                //test if article has title and text
                if (!articleJson.has("title") || !articleJson.get("title").isTextual() || articleJson.get("title").asText().isEmpty()) {
                    return null;
                }

                if (!articleJson.has("text") || !articleJson.get("text").isTextual() || articleJson.get("text").asText().isEmpty()) {

                    return null;
                }


                article.setTitle(articleJson.get("title").asText());
                article.setText(articleJson.get("text").asText());


                if (articleJson.has("images")) {
                    List<Element> elementList = new ArrayList<>();
                    Iterator<JsonNode> images = articleJson.get("images").elements();
                    while (images.hasNext()) {
                        JsonNode imageJson = images.next();

                        ImageElement image = new ImageElement();
                        image.setUrl(imageJson.get("url").asText());
                        if (imageJson.has("primary")) {
                            image.setPrimary(imageJson.get("primary").asBoolean());
                        } else {
                            image.setPrimary(false);
                        }
                        elementList.add(image);
                    }
                    article.setElements(elementList);
                }
            }
            //make sure it is fully consumed and cloased
            EntityUtils.consumeQuietly(entity);
        } catch (Exception e) {
            logger.error("Unable to extract artilce", e);
            return null;
        } finally {
            if (httpget != null) {
                httpget.releaseConnection();
            }
        }

        return article;
    }


}
