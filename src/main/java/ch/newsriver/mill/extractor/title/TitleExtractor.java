package ch.newsriver.mill.extractor.title;

import ch.newsriver.data.url.BaseURL;
import ch.newsriver.data.url.FeedURL;
import ch.newsriver.mill.extractor.metadata.MetaDataExtractor;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

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

    private static final float MIN_TITLE_PERMUTATION_LENGTH = 0.25f;
    private static String[][] UMLAUT_REPLACEMENTS = {{"Ä", "Ae"}, {"Ü", "Ue"}, {"Ö", "Oe"}, {"ä", "ae"}, {"ü", "ue"}, {"ö", "oe"}, {"ß", "ss"}};
    private Document doc;
    private List<BaseURL> referrals;
    private URL url;

    public TitleExtractor(Document doc, URL url, List<BaseURL> referrals) {
        this.doc = doc;
        this.url = url;
        this.referrals = referrals;
    }

    public String extractTitle() {


        Set<String> mainTitleCandidates = new HashSet<>();

        mainTitleCandidates.add(normaliseHTMLText(doc.select("title").text()));
        Iterator<Element> h1s = doc.select("h1").iterator();
        while (h1s.hasNext()) {
            mainTitleCandidates.add(normaliseHTMLText(h1s.next().text()));
        }


        Map<String, Integer> alternatives = new HashMap<>();

        Iterator<Element> hTags = doc.select("h1, h2").iterator();
        while (hTags.hasNext()) {
            alternatives.put(hTags.next().text(), 1);
        }
        hTags = doc.select("h3, h4, h5, h6").iterator();
        while (hTags.hasNext()) {
            alternatives.put(hTags.next().text(), 1);
        }

        MetaDataExtractor metaDataExtractor = new MetaDataExtractor(doc);
        metaDataExtractor.extractMetaOpenGraph().getTitle().map(t -> alternatives.put(normaliseHTMLText(t), 1));
        metaDataExtractor.extractMetaTwitter().getTitle().map(t -> alternatives.put(normaliseHTMLText(t), 1));

        alternatives.put(url.getPath(), 1);
        alternatives.put(normaliseHTMLText(doc.select("title").text()), 1);

        if (referrals != null) {
            for (BaseURL url : referrals) {
                if (url instanceof FeedURL) {
                    //referral titles are very important, give twice the weight
                    alternatives.put(normaliseHTMLText(((FeedURL) url).getTitle()), 2);
                }
            }
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
        return text.replaceAll("\\u00a0+", " ").replaceAll("[\\s]+", " ");
    }

    protected String normaliseString(String string) {
        for (int i = 0; i < UMLAUT_REPLACEMENTS.length; i++) {
            string = string.replaceAll(UMLAUT_REPLACEMENTS[i][0], UMLAUT_REPLACEMENTS[i][1]);
        }
        return Normalizer.normalize(string, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "").toUpperCase().replaceAll("[^\\p{L}]+", " ");
    }


    public PermutationScore processTitle(String refTitle, Map<String, Integer> alternatives) {


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

