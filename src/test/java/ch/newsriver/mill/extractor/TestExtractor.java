package ch.newsriver.mill.extractor;

import ch.newsriver.mill.extractor.title.TitleExtractor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.intenthq.gander.Gander;
import com.intenthq.gander.PageInfo;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * Created by eliapalme on 12/09/16.
 */
@RunWith(Parameterized.class)
public class TestExtractor {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    FormatDateTimeFormatter esDateTimeFormatter = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
    private WebpageToTest webpage;
    private Document doc;


    public TestExtractor(WebpageToTest webPage, String hostname) {
        this.webpage = webPage;
    }

    @Parameterized.Parameters(name = "{index}: {1}")
    public static Collection getWebPages() throws IOException {
        Collection<Object[]> webPages = new ArrayList<>();


        ClassLoader cl = TestExtractor.class.getClassLoader();
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(cl);
        Resource[] resources = resolver.getResources("classpath*:/**/extractor/*.json");
        for (Resource resource : resources) {
            WebpageToTest website = mapper.readValue(resource.getFile(), WebpageToTest.class);
            if (website.getSource() == null) {
                website.setSource(download(website.getUrl()));
                mapper.writeValue(new File(resource.getURI().getPath().replace("build/resources/test", "src/test/resources")), website);
            }
            webPages.add(new Object[]{website, new URL(website.getUrl()).getHost()});
            Document doc = Jsoup.parse(website.getSource(), website.getUrl());
        }
        return webPages;
    }

    private static String download(String url) throws IOException {
        String source;
        try (InputStream in = new URL(url).openStream()) {
            source = IOUtils.toString(in);
        }
        return source;
    }

    @Before
    public void parseHTMLDoc() {
        doc = Jsoup.parse(this.webpage.getSource(), this.webpage.getUrl());
    }

    @Test
    public void testTitleExtraction() throws IOException {
        TitleExtractor titleExtractor = new TitleExtractor(this.doc, this.webpage.getUrl(), this.webpage.getReferral());
        assertEquals(this.webpage.getTitle(), titleExtractor.extractTitle());
    }

    @Test
    public void testPublicationDateExtraction() {
        if (this.webpage.getPublishDate() != null) {
            PageInfo pageInfo = Gander.extract(this.webpage.getSource(), "all").get();
            String date = simpleDateFormat.format(pageInfo.publishDate().get());
            try {
                esDateTimeFormatter.parser().parseMillis(date);
            } catch (IllegalArgumentException e) {
                date = null;
            }
            assertEquals(this.webpage.getPublishDate(), date);

        }
    }

}
