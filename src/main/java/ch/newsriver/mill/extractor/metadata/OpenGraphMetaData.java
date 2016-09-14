package ch.newsriver.mill.extractor.metadata;


import org.jsoup.nodes.Document;

import java.util.Optional;

/**
 * Created by eliapalme on 11/09/16.
 */
public class OpenGraphMetaData extends MetaDataExtractor {

    private Optional<String> title;
    private Optional<String> siteName;
    private Optional<String> url;
    private Optional<String> description;
    private Optional<String> image;
    private Optional<String> type;
    private Optional<String> locale;
    private Optional<String> publishedTime;

    protected OpenGraphMetaData(Document doc) {
        super(doc);
        this.title = this.metaContent("og:title");
        this.siteName = this.metaContent("og:site_name");
        this.url = this.metaContent("og:url");
        this.description = this.metaContent("og:description");
        this.image = this.metaContent("og:image");
        this.type = this.metaContent("og:type");
        this.locale = this.metaContent("og:locale");
        this.publishedTime = this.metaContent("article:published_time");
    }

    public Optional<String> getTitle() {
        return title;
    }

    public Optional<String> getSiteName() {
        return siteName;
    }

    public Optional<String> getUrl() {
        return url;
    }

    public Optional<String> getDescription() {
        return description;
    }

    public Optional<String> getImage() {
        return image;
    }

    public Optional<String> getType() {
        return type;
    }

    public Optional<String> getLocale() {
        return locale;
    }

    public Optional<String> getPublishedTime() {
        return publishedTime;
    }

}
