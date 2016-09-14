package ch.newsriver.mill.extractor;

import ch.newsriver.data.content.Article;
import ch.newsriver.data.html.HTML;
import ch.newsriver.mill.extractor.title.TitleExtractor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 * Created by eliapalme on 18/03/16.
 */
public abstract class ArticleExtractor {

    private static final Logger logger = LogManager.getLogger(ArticleExtractor.class);


    public Article extract(HTML html) {

        Article article = extractArticle(html);
        if (article == null) {
            return null;
        }

        Document doc = Jsoup.parse(html.getRawHTML());
        TitleExtractor titleExtractor = new TitleExtractor(doc, html.getUrl(), html.getReferral());
        article.setTitle(titleExtractor.extractTitle());

        article.setLanguage(html.getLanguage());
        article.getReferrals().add(html.getReferral());
        article.setUrl(html.getUrl());
        article.setDiscoverDate(html.getReferral().getDiscoverDate());
        return article;
    }

    abstract public Article extractArticle(HTML html);


}
