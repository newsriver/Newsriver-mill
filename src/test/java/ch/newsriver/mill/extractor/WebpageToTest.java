package ch.newsriver.mill.extractor;

import ch.newsriver.data.url.BaseURL;

import java.util.List;

/**
 * Created by eliapalme on 12/09/16.
 */
public class WebpageToTest {


    private String title;
    private String url;
    private String source;
    private List<BaseURL> referrals;

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

    public List<BaseURL> getReferrals() {
        return referrals;
    }

    public void setReferrals(List<BaseURL> referrals) {
        this.referrals = referrals;
    }
}
