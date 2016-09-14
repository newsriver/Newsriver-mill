package ch.newsriver.mill.extractor.title;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by eliapalme on 11/09/16.
 */
public class TestTitleExtractionStatic {

    private TitleExtractor titleExtractor = new TitleExtractor(null, null, null);

    @Test
    public void testApostropheNormalization() {
        assertEquals(titleExtractor.normaliseString("laissé à l'école"), "LAISSE A L ECOLE");
        assertEquals(titleExtractor.normaliseString("laissé à l’école"), "LAISSE A L ECOLE");
    }

    @Test
    public void testTextNormalization() {
        assertEquals(titleExtractor.normaliseString("tèst"), "TEST");
        assertEquals(titleExtractor.normaliseString("çasa"), "CASA");
        assertEquals(titleExtractor.normaliseString("çasâ"), "CASA");
        assertEquals(titleExtractor.normaliseString("Zürich"), "ZUERICH");
        assertEquals(titleExtractor.normaliseString("Zürich/tèst"), "ZUERICH TEST");
        assertEquals(titleExtractor.normaliseString("<tèst>"), " TEST ");
    }


    @Test
    public void testTitleExtractionWithSymbols() {
        String rawTitle = "I like: the nèw title extractor - newsriver.io";
        Map<String, Integer> alternatives = new HashMap<>();
        alternatives.put("Re-tweet: \"I like: the new title extractor\"", 1);
        alternatives.put("Share <I LIKE: the new title extractor>", 1);

        assertEquals("I like: the nèw title extractor", titleExtractor.processTitle(rawTitle, alternatives).getPermutation());
    }

    @Test
    public void testTitleExtractionWithSymbols2() {
        String rawTitle = "What is good code? A scientific definition. - Intent HQ Engineering blog";
        Map<String, Integer> alternatives = new HashMap<>();
        //alternatives.put("What is good code? A scientific definition. - Intent HQ Engineering blog", 1);
        alternatives.put("What is good code? A scientific definition.", 1);

        assertEquals("What is good code? A scientific definition.", titleExtractor.processTitle(rawTitle, alternatives).getPermutation());
    }


    @Test
    public void testTitleExtractionWithAccents() {
        String rawTitle = "title: | Zürich is cool-NZZ";
        Map<String, Integer> alternatives = new HashMap<>();
        alternatives.put("Zürich is cool", 1);
        alternatives.put("zurich-is-cool", 1);

        assertEquals("Zürich is cool", titleExtractor.processTitle(rawTitle, alternatives).getPermutation());
    }

    @Test
    public void testTitleExtractionWithURL() {
        String rawTitle = "Zürich the city | Blick new";
        Map<String, Integer> alternatives = new HashMap<>();
        alternatives.put("http://news.com/zuerich-the-city", 1);

        assertEquals("Zürich the city", titleExtractor.processTitle(rawTitle, alternatives).getPermutation());
    }

}
