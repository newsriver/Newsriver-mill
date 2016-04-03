package ch.newsriver.mill.extractor.publisher;

import ch.newsriver.data.publisher.Publisher;
import ch.newsriver.data.html.HTML;
import ch.newsriver.data.publisher.PublisherFactory;
import ch.newsriver.data.website.alexa.AlexaClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.net.*;

/**
 * Created by eliapalme on 21/03/16.
 */
public class PublisherExtractor {

    private static final Logger logger = LogManager.getLogger(PublisherExtractor.class);




        public PublisherExtractor() {

        }

        public Publisher extract(HTML html){


            String domain = extractDomain(html.getUrl());
            if(domain==null || domain.isEmpty()){
                return null;
            }

            Publisher publisher = PublisherFactory.getInstance().getPublisher(domain);

            if(publisher==null){
                publisher = new Publisher();
                publisher.setDomainName(domain);
                AlexaClient alexaClient = new AlexaClient();
                publisher.setSiteInfo(alexaClient.getSiteInfo(domain));
            }

            publisher.getHosts().add(extractHost(html.getUrl()));
            return publisher;
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

    public String extractHost(String url){

        try {
            URI uri = new URI(url);
            String host = uri.getHost();
            return  host;
        }catch (URISyntaxException ex){
            logger.error("Invalid URL syntax",ex);
        }
        return  null;
    }


    
}
