package ch.newsriver.mill.extractor.metadata;


import org.jsoup.nodes.Document;

import java.util.Optional;

/**
 * Created by eliapalme on 11/09/16.
 */
public class TwitterMetaData extends MetaDataExtractor {

    private Optional<String> title;
    private Optional<String> siteName;
    private Optional<String> creator;
    private Optional<String> description;
    private Optional<String> image;

    protected TwitterMetaData(Document doc) {
        super(doc);
        this.title = this.metaContent("twitter:title");
        this.siteName = this.metaContent("twitter:site");
        this.creator = this.metaContent("twitter:creator");
        this.description = this.metaContent("twitter:description");
        this.image = this.metaContent("twitter:image");

    }

    public Optional<String> getTitle() {
        return title;
    }

    public Optional<String> getSiteName() {
        return siteName;
    }

    public Optional<String> getCreator() {
        return creator;
    }

    public Optional<String> getDescription() {
        return description;
    }

    public Optional<String> getImage() {
        return image;
    }
}


