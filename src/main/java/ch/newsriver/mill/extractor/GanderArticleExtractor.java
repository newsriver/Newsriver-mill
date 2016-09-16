package ch.newsriver.mill.extractor;

import ch.newsriver.data.content.Article;
import ch.newsriver.data.content.ImageElement;
import ch.newsriver.data.html.HTML;
import com.intenthq.gander.Gander;
import com.intenthq.gander.PageInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.index.mapper.core.DateFieldMapper;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by eliapalme on 18/03/16.
 */
public class GanderArticleExtractor extends ArticleExtractor {

    private static final Logger logger = LogManager.getLogger(GanderArticleExtractor.class);
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    FormatDateTimeFormatter esDateTimeFormatter = DateFieldMapper.Defaults.DATE_TIME_FORMATTER;


    public Article extractArticle(HTML html) {

        if (html.getLanguage() == null) {
            return null;
        }

        PageInfo pageInfo = null;
        try {
            pageInfo = Gander.extract(html.getRawHTML(), html.getLanguage()).get();

        } catch (Exception ex) {
            logger.error("Unable to extract article content", ex);
            return null;
        }

        if (!pageInfo.cleanedText().isDefined()) {
            return null;
        }

        Article article = new Article();
        article.setText(pageInfo.cleanedText().get());
        article.setTitle(pageInfo.processedTitle());
        article.setStructuredText(pageInfo.structuredText().get());

        if (pageInfo.publishDate().isDefined()) {
            article.setPublishDate(simpleDateFormat.format(pageInfo.publishDate().get()));
            //test if date is parsable, this methods avoids to pass invalid dates to ES
            try{
                esDateTimeFormatter.parser().parseMillis(article.getPublishDate());
            }catch (IllegalArgumentException e){
                logger.error("Publish date is invalid");
                article.setPublishDate(null);
            }
        }

        if (pageInfo.openGraphData() != null && pageInfo.openGraphData().image() != null && pageInfo.openGraphData().image().isDefined()) {
            ImageElement image = new ImageElement();
            image.setUrl(pageInfo.openGraphData().image().get().toString());
            image.setPrimary(true);
            article.getElements().add(image);
        }


        return article;
    }
}
