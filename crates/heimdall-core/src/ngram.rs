/// ngram.rs — trigram substring index.
///
/// The exact and phonetic FSTs answer "is THIS string a known key" — they
/// can't help with truncations like "Stadsbib" → "Stadsbiblioteket" or
/// abbreviations like "Centralstat Stockholm" → "Stockholm Central
/// Station". Levenshtein covers single-character typos but its edit budget
/// scales with query length and an 8-char query against a 16-char target
/// is way out of its 1-2 edit window.
///
/// The trigram index plugs that gap. We chop every indexed name into all
/// 3-character substrings (with `^` and `$` boundary markers, so prefix /
/// suffix matches get a small boost) and write them to a second FST whose
/// values point into a posting-list sidecar. At query time we trigram the
/// query the same way, look each trigram up, and union the candidate
/// records — ranking by `matched_trigrams / query_trigrams * importance`.
///
/// File layout (parallel to the exact FST):
///
///   fst_ngram.fst       trigram → posting offset
///   record_lists_ngram.bin   `[u16 count][u32 record_id]*count` per posting
///
/// Same posting-list format as `record_lists.bin` for the exact FST so the
/// existing `decode_posting_list` reader works unchanged.

/// Generate the canonical set of trigrams for a normalized lowercase string.
///
/// Rules:
/// - Insert `^` at the start of each whitespace-separated token and `$` at
///   the end. This means "vasa museet" produces `^va`, `vas`, `asa`, `sa$`,
///   `^mu`, `mus`, `use`, `see`, `eet`, `et$`. Boundary trigrams give a
///   prefix-match signal — a query "vasamus" generates `^va` … `mus` … so
///   "Vasamuseet" with `^va` and `mus` scores higher than an unrelated
///   record that only happens to contain `vas`.
/// - Tokens of 1-2 characters are still emitted with boundaries; they
///   collapse into a single boundary-pair trigram (e.g. "i" → `^i$`).
/// - Output is deduplicated within a single name so common letter pairs
///   don't get double-counted (e.g. "tatata" yields `^ta`, `tat`, `ata`,
///   `ta$` — not three copies of `ata`).
///
/// Returns trigrams as owned String — they're written to a TSV at build
/// time and only allocated once per name.
pub fn trigrams(name: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for token in name.split_whitespace() {
        if token.is_empty() { continue; }
        // Bracket the token with sentinel chars so prefix and suffix
        // trigrams are distinguishable from interior ones.
        let bracketed: String = format!("^{}$", token);
        let chars: Vec<char> = bracketed.chars().collect();
        if chars.len() < 3 {
            // Single-char token like "i" → "^i$". Still useful as one
            // trigram so the search "i" doesn't produce zero output.
            out.push(bracketed);
            continue;
        }
        for w in chars.windows(3) {
            let s: String = w.iter().collect();
            out.push(s);
        }
    }
    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_trigrams() {
        let t = trigrams("vasa");
        // ^va, vas, asa, sa$
        assert!(t.contains(&"^va".to_string()));
        assert!(t.contains(&"vas".to_string()));
        assert!(t.contains(&"asa".to_string()));
        assert!(t.contains(&"sa$".to_string()));
    }

    #[test]
    fn test_short_token_boundary() {
        let t = trigrams("i");
        assert_eq!(t, vec!["^i$".to_string()]);
    }

    #[test]
    fn test_multi_word() {
        let t = trigrams("vasa museet");
        // Expect ^va, vas, asa, sa$, ^mu, mus, use, see, eet, et$
        for expected in &["^va", "vas", "asa", "sa$", "^mu", "mus", "use", "see", "eet", "et$"] {
            assert!(t.contains(&expected.to_string()), "missing {}", expected);
        }
    }

    #[test]
    fn test_dedup() {
        let t = trigrams("aaaaa");
        // "^aaaa$" → ^aa, aaa, aaa, aaa, aa$ → unique: ^aa, aaa, aa$
        assert_eq!(t, vec!["^aa".to_string(), "aa$".to_string(), "aaa".to_string()]);
    }
}
