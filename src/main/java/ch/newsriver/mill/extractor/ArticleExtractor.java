package ch.newsriver.mill.extractor;

import ch.newsriver.data.content.Article;
import ch.newsriver.data.publisher.Publisher;
import ch.newsriver.data.html.HTML;
import ch.newsriver.mill.extractor.publisher.PublisherExtractor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by eliapalme on 18/03/16.
 */
public abstract class ArticleExtractor {

    private static final Logger logger = LogManager.getLogger(ArticleExtractor.class);


     public Article extract(HTML html){

         Article article = extractArticle(html);
         if(article==null){
             return null;
         }
         article.setPublisher(extractPublisher(html));
         article.setLanguage(html.getLanguage());
         article.getReferrals().add(html.getReferral());
         article.setUrl(html.getUrl());
         article.setDiscoverDate(html.getReferral().getDiscoverDate());
         article.setHtml(html.getRawHTML());
         return article;
     }

    abstract public Article extractArticle(HTML html);



    public Publisher extractPublisher(HTML html){

        PublisherExtractor publisherExtractor = new PublisherExtractor();
        return publisherExtractor.extract(html);
    }


    public String extractDomain(String url){

        try {
            URI uri = new URI(url);
            String host = uri.getHost();

            int rootI = host.lastIndexOf(".");
            if (rootI < 0) {
                return host;
            }
            int domainI = host.substring(0, rootI).lastIndexOf(".");
            if (domainI < 0 || domainI+1 >= host.length()) {
                return host;
            }
            return host.substring(domainI+1, host.length());
        }catch (URISyntaxException ex){
            logger.error("Invalid URL syntax",ex);
        }
        return  null;
    }

}
