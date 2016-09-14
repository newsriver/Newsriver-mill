package ch.newsriver.mill.extractor.metadata;

import org.jsoup.nodes.Document;

import java.util.Optional;

/**
 * Created by eliapalme on 11/09/16.
 */
public class MetaDataExtractor {


    private Document doc;

    public MetaDataExtractor(Document doc) {
        this.doc = doc;
    }

    public String extractMetaDescription() {
        return metaContent("description")
                .orElse(metaContent("og:description")
                        .orElse(metaContent("twitter:description")
                                .orElse("")
                        )).trim();
    }

    public OpenGraphMetaData extractMetaOpenGraph() {
        return new OpenGraphMetaData(this.doc);
    }

    public TwitterMetaData extractMetaTwitter() {
        return new TwitterMetaData(this.doc);
    }

    public String extractMetaKeywords() {
        return metaContent("keywords")
                .orElse(metaContent("news_keywords")
                        .orElse("")
                ).trim();
    }


    protected Optional<String> metaContent(String metaName) {

        return doc.select("meta[property=" + metaName + "], meta[name=" + metaName + "]").stream()
                .findFirst()
                .map(e -> e.attr("content"));
    }

}
