package ch.newsriver.mill.extractor.title;

import ch.newsriver.data.url.BaseURL;
import ch.newsriver.data.url.FeedURL;
import ch.newsriver.mill.extractor.metadata.MetaDataExtractor;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by eliapalme on 11/09/16.
 */
public class TitleExtractor {
    private static final Logger logger = LogManager.getLogger(TitleExtractor.class);


    private static final float MIN_TITLE_PERMUTATION_LENGTH = 0.33f; //the higher the less permutations are generated
    private static String[][] REPLACEMENTS = {{"’", "'"}, {"Ä", "Ae"}, {"Ü", "Ue"}, {"Ö", "Oe"}, {"ä", "ae"}, {"ü", "ue"}, {"ö", "oe"}, {"ß", "ss"}};
    private Document doc;
    private BaseURL referral;
    private String url;

    public TitleExtractor(Document doc, String url, BaseURL referral) {
        this.doc = doc;
        this.url = url;
        this.referral = referral;
    }

    public String extractTitle() {

        Set<String> mainTitleCandidates = new HashSet<>();
        Map<String, Integer> alternatives = new HashMap<>();

        //Find primary title

        if (doc.select("title") != null && doc.select("title").hasText()) {
            String titleTag = normaliseHTMLText(doc.select("title").text());
            //add the tile to both main and alternatives titles
            //it is added to alternative as well because he is an alternative to other main candidates
            mainTitleCandidates.add(titleTag);
            alternatives.put(titleTag, 1);
        }


        Iterator<Element> h1s = doc.select("h1").iterator();
        while (h1s.hasNext()) {
            String h1Tag = normaliseHTMLText(h1s.next().text());
            mainTitleCandidates.add(h1Tag);
            alternatives.put(h1Tag, 1);
        }


        //Find alternative titles permutations

        //add all H tags
        Iterator<Element> hTags = doc.select("h2,h3, h4, h5, h6").iterator();
        while (hTags.hasNext()) {
            alternatives.put(normaliseHTMLText(hTags.next().text()), 1);
        }

        //add all meta titles
        MetaDataExtractor metaDataExtractor = new MetaDataExtractor(doc);
        metaDataExtractor.extractMetaOpenGraph().getTitle().map(t -> alternatives.put(normaliseHTMLText(t), 1));
        metaDataExtractor.extractMetaTwitter().getTitle().map(t -> alternatives.put(normaliseHTMLText(t), 1));

        //add URL path
        try {
            alternatives.put(new URL(url).getPath(), 1);
        } catch (MalformedURLException e) {
            logger.error("Invalid article URL", e);
        }

        //add all referrals titles or link text
        if (referral != null) {
            if (referral instanceof FeedURL) {
                //referral titles are very important, give twice the weight
                alternatives.put(normaliseHTMLText(((FeedURL) referral).getTitle()), 2);
            }
            //TODO: consider adding LinkURL text as altrenative title
        }


        String title = "";
        int score = 0;
        for (String mainCandidate : mainTitleCandidates) {
            PermutationScore permutation = processTitle(mainCandidate, alternatives);
            if (permutation.score > score) {
                title = permutation.getPermutation();
                score = permutation.getScore();
            }
        }

        return title;
    }

    protected String normaliseHTMLText(String text) {
        if (text == null) return null;
        return text.replaceAll("\\u00a0+", " ").replaceAll("[\\s]+", " ");
    }

    protected String normaliseString(String string) {

        //replace special chars with normalise version
        //for example ß needs to be converted to ss, doing so will add one char to the string an make
        //sure it matches with version of the same text that are not normalised.
        for (int i = 0; i < REPLACEMENTS.length; i++) {
            string = string.replaceAll(REPLACEMENTS[i][0], REPLACEMENTS[i][1]);
        }
        return Normalizer.normalize(string, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "").toUpperCase().replaceAll("[^\\p{L}]+", " ");
    }


    protected PermutationScore processTitle(String refTitle, Map<String, Integer> alternatives) {


        Map<String, Integer> permutationScores = new HashMap<>();

        //create map with normalised title permutations as key and original version as value
        Map<String, String> originalTitleMap = new HashMap<>();
        for (String referencePermutation : stringPermutations(refTitle, false)) {

            //remove undesired leading and tail chars from the referencePermutation
            referencePermutation = StringUtils.strip(referencePermutation, " –_");
            //normalise text and strip it
            String normalised = StringUtils.strip(normaliseString(referencePermutation), " –_");

            //if keys collide keep the longer version
            if (!(originalTitleMap.containsKey(normalised) &&
                    originalTitleMap.get(normalised).length() >= referencePermutation.length())) {
                originalTitleMap.put(normalised, referencePermutation);
                addOrUpdate(permutationScores, normalised, 1, 0);
            }
        }

        //update the permutation scores map for every permutation found in a alternative title
        for (String alternative : alternatives.keySet()) {
            updatePermutationScore(permutationScores, alternative, alternatives.get(alternative));
        }

        //Sort the permutation score map and get the highest scored permutation
        String title = permutationScores.entrySet()
                .stream()
                .sorted((left, right) -> {
                    if (left.getValue() == right.getValue()) {
                        return Long.compare(right.getKey().length(), left.getKey().length());
                    } else {
                        return Long.compare(right.getValue(), left.getValue());
                    }
                }).collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                )).keySet().iterator().next();


        PermutationScore permutation = new PermutationScore();
        permutation.setPermutation(originalTitleMap.get(title));
        permutation.setScore(permutationScores.get(title));

        return permutation;
    }

    private void updatePermutationScore(Map<String, Integer> permutations, String alternative, Integer alternativeScore) {

        //Sort permutations by length and find the first matching
        List<String> sortedPermutations = new ArrayList<>(stringPermutations(alternative, true));
        sortedPermutations.sort((p1, p2) -> Long.compare(p2.length(), p1.length()));
        for (String permutation : sortedPermutations) {

            if (addOrUpdate(permutations, permutation, null, alternativeScore)) {
                return;
            }
        }
    }


    protected Set<String> stringPermutations(String title, boolean normalise) {
        Set<String> permutations = new HashSet<>();
        String[] keywords = title.split("((?<=[^\\p{L}])|(?=[^\\p{L}]))");

        for (int i = 0; i < keywords.length; i++) {
            String permutation = "";
            for (int j = i; j < keywords.length; j++) {
                permutation = permutation.concat(keywords[j]);
                //too small permutation are causing problems as more two words may occur in the same sentence
                //therefore permutations need to have a min length
                if (!permutation.isEmpty() && permutation.length() > title.length() * MIN_TITLE_PERMUTATION_LENGTH) {
                    if (normalise) {
                        permutations.add(StringUtils.strip(normaliseString(permutation)));
                    } else {
                        permutations.add(StringUtils.strip(permutation));
                    }
                }
            }
        }
        return permutations;
    }


    private boolean addOrUpdate(Map<String, Integer> map, String key, Integer newValue, Integer updateValue) {
        if (map.containsKey(key)) {
            map.put(key, map.get(key) + updateValue);
            return true;
        } else {
            if (newValue != null) map.put(key, newValue);
            return false;
        }
    }

    public static class PermutationScore {

        String permutation;
        int score;

        public String getPermutation() {
            return permutation;
        }

        public void setPermutation(String permutation) {
            this.permutation = permutation;
        }

        public int getScore() {
            return score;
        }

        public void setScore(int score) {
            this.score = score;
        }
    }


}

