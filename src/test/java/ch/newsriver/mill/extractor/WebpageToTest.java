package ch.newsriver.mill.extractor;

import ch.newsriver.data.url.BaseURL;

/**
 * Created by eliapalme on 12/09/16.
 */
public class WebpageToTest {


    private String title;
    private String url;
    private String source;
    private BaseURL referral;
    private String publishDate;
    private String text;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }


    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public BaseURL getReferral() {
        return referral;
    }

    public void setReferral(BaseURL referral) {
        this.referral = referral;
    }


    public String getPublishDate() {
        return publishDate;
    }

    public void setPublishDate(String publishDate) {
        this.publishDate = publishDate;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}
