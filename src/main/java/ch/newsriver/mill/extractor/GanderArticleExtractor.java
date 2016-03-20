package ch.newsriver.mill.extractor;

import ch.newsriver.data.content.Article;
import ch.newsriver.data.html.HTML;
import com.intenthq.gander.Gander;
import com.intenthq.gander.PageInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;

/**
 * Created by eliapalme on 18/03/16.
 */
public class GanderArticleExtractor extends ArticleExtractor{

    private static final Logger logger = LogManager.getLogger(GanderArticleExtractor.class);
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    public Article extractArticle(HTML html){

        if(html.getLanguage() == null){
            return null;
        }

        PageInfo pageInfo = null;
        try {
            pageInfo = Gander.extract(html.getRawHTML(), html.getLanguage()).get();

        } catch (Exception ex) {
            logger.error("Unable to extract article content",ex);
            return  null;
        }

        if(!pageInfo.cleanedText().isDefined()){
            return null;
        }

        Article article = new Article();
        article.setText(pageInfo.cleanedText().get());
        article.setTitle(pageInfo.processedTitle());
        if(pageInfo.publishDate().isDefined()){
            article.setPublishDate(simpleDateFormat.format(pageInfo.publishDate().get()));
        }

        return article;
    }
}
