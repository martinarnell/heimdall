/// addr_index.rs — Address lookup index
///
/// Opens the address FST and record store, provides address geocoding.
///
/// Query flow:
///   1. Parse input → AddressQuery (street, housenumber, city)
///   2. If city present, resolve to municipality_id via place FST
///   3. Look up "{normalized_street}:{muni_id}:{housenumber}" in address FST
///   4. Fall back to wildcard "{normalized_street}:0:{housenumber}" if no city

use std::path::Path;
use fst::Map;

use crate::types::*;
use crate::addr_store::{AddrStore, HouseEntry};
use crate::error::HeimdallError;
use crate::compressed_io;
use crate::reverse::GeohashIndex;

// ---------------------------------------------------------------------------
// Address query parsing
// ---------------------------------------------------------------------------

/// Parsed address query
#[derive(Debug, Clone)]
pub struct AddressQuery {
    pub street: String,
    pub housenumber: String,
    pub city: Option<String>,
    pub postcode: Option<String>,
}

/// Try to parse a free-text query as a street-only query (no housenumber).
/// Returns Some only if there's a comma-separated city part.
///
/// Patterns handled:
///   "Hauptstraße, Berlin"       → street=hauptstraße, city=Berlin
///   "Kungsgatan, Stockholm"     → street=kungsgatan, city=Stockholm
///   "Berlin"                    → None (no comma, no street)
pub fn parse_street_query(input: &str) -> Option<(String, String)> {
    let input = input.trim();
    if !input.contains(',') { return None; }

    let parts: Vec<&str> = input.splitn(2, ',').collect();
    if parts.len() != 2 { return None; }

    let street = parts[0].trim();
    let city = parts[1].trim();

    // Must have both a street and city part, and the street should be at least 2 chars
    if street.len() < 2 || city.is_empty() { return None; }

    // Street part should not start with a digit (that would be a housenumber, not a street)
    if street.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(true) { return None; }

    Some((street.to_lowercase(), city.to_owned()))
}

/// Try to parse a free-text query as an address.
/// Returns None if it doesn't look like an address query.
///
/// Patterns handled:
///   "Kungsgatan 15, Stockholm"       → street=Kungsgatan, number=15, city=Stockholm
///   "Kungsgatan 15 Stockholm"        → street=Kungsgatan, number=15, city=Stockholm
///   "Kungsgatan 15B"                 → street=Kungsgatan, number=15B
///   "Kungsgatan 15-17"               → street=Kungsgatan, number=15
///   "Kungsgatan 15, 11456 Stockholm" → street=Kungsgatan, number=15, postcode=11456
pub fn parse_address_query(input: &str) -> Option<AddressQuery> {
    let input = input.trim();
    if input.is_empty() { return None; }

    // Split on comma first
    let parts: Vec<&str> = input.splitn(2, ',').collect();
    let street_part = parts[0].trim();
    let city_part = if parts.len() > 1 { Some(parts[1].trim()) } else { None };

    // In the street part, find where the house number starts.
    // House number is the first standalone digit group after alphabetic text.
    let (street, number) = split_street_number(street_part)?;

    if street.is_empty() || number.is_empty() {
        return None;
    }

    // Parse city/postcode from the part after the comma (or remaining words)
    let (city, postcode) = if let Some(cp) = city_part {
        parse_city_postcode(cp)
    } else {
        // No comma — check if there are words after the number in the original string
        // "Kungsgatan 15 Stockholm" → city is the trailing word(s)
        let after_number = extract_after_number(street_part, &street, &number);
        if let Some(trailing) = after_number {
            (Some(trailing), None)
        } else {
            (None, None)
        }
    };

    Some(AddressQuery {
        street: street.to_lowercase(),
        housenumber: normalize_housenumber(&number),
        city,
        postcode,
    })
}

/// Split a street-and-number phrase into (street, number).
///
/// Handles both number conventions:
///   number-after  ("Kungsgatan 15B")  — Nordic, English, Dutch
///   number-first  ("10 Rue de Rivoli", "10bis avenue Foch") — French,
///                                Spanish, Italian, Belgian
///
/// Returns None when the input is just a street with no number, or just
/// a postcode/number with no street to attach it to.
fn split_street_number(s: &str) -> Option<(String, String)> {
    let words: Vec<&str> = s.split_whitespace().collect();
    if words.len() < 2 { return None; }

    // ── Number-first: "10 rue ...", "10bis avenue ...", "1ter rue ..."
    // The first word starts with a digit and the rest looks like a
    // recognisable street thoroughfare. We require a known street-type
    // keyword in words[1..] so a stray "75001 Paris" or "100 something"
    // doesn't get mis-parsed as an address.
    if words[0].chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false)
        && looks_like_house_number(words[0])
        && words.len() >= 3
        && contains_street_keyword(&words[1..])
    {
        return Some((words[1..].join(" "), words[0].to_owned()));
    }

    // ── Number-after: "Kungsgatan 15B", "Friedrichstraße 100"
    // Find the first word that starts with a digit at index >= 1.
    for i in 1..words.len() {
        let word = words[i];
        if word.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) {
            let street = words[..i].join(" ");
            let number = word.to_owned();
            return Some((street, number));
        }
    }

    None
}

/// True when the token plausibly *is* a house number (1, 12, 5b, 10bis,
/// 1ter). Plain integers up to 4 digits qualify; longer strings are
/// rejected to prevent postcodes (75001) from matching. Trailing
/// alphabetic suffixes ("b", "bis", "ter", "quater") common in Nordic /
/// Romance numbering are accepted.
fn looks_like_house_number(s: &str) -> bool {
    if s.is_empty() { return false; }
    let bytes = s.as_bytes();
    if !bytes[0].is_ascii_digit() { return false; }
    let digit_end = bytes.iter().position(|b| !b.is_ascii_digit()).unwrap_or(bytes.len());
    if digit_end == 0 || digit_end > 4 { return false; }
    if digit_end == bytes.len() { return true; }
    // Trailing must be a short alpha suffix
    let suffix = &s[digit_end..];
    if suffix.len() > 6 { return false; }
    suffix.chars().all(|c| c.is_ascii_alphabetic())
}

/// True when any of these tokens is a recognised street-type keyword.
/// Multilingual (we run on the same parser regardless of country): French
/// (rue/avenue/boulevard/...), German (straße/strasse/platz/...), English,
/// Italian, Spanish, Dutch. Match is case-insensitive on the first
/// thoroughfare-only token; we don't accept "saint" / "saint-" alone since
/// those are part of place names, not street types.
fn contains_street_keyword(tokens: &[&str]) -> bool {
    const STREET_KEYWORDS: &[&str] = &[
        // French
        "rue", "avenue", "av", "av.", "boulevard", "bd", "bd.", "blvd",
        "allée", "allee", "all", "all.", "chemin", "ch", "ch.",
        "impasse", "imp", "imp.", "place", "pl", "pl.", "passage", "pass",
        "route", "rte", "voie", "quai", "qu", "qu.", "cours", "crs",
        "promenade", "prom", "prom.", "esplanade", "rond-point",
        "square", "sq", "sq.", "cité", "cite", "villa", "venelle",
        "sentier", "ruelle", "faubourg", "faub", "fbg", "parvis",
        // German
        "straße", "strasse", "str.", "str", "weg", "platz", "gasse",
        "allee", "ufer", "damm", "ring", "chaussee", "promenade", "graben",
        "markt", "steig", "stieg", "brücke", "brucke",
        // English
        "street", "st", "st.", "road", "rd", "rd.", "lane", "ln",
        "drive", "dr", "dr.", "court", "ct", "way",
        "highway", "hwy", "parkway", "pkwy", "terrace", "ter",
        "circle", "cir", "trail", "tr", "tr.",
        // Italian
        "via", "viale", "piazza", "pza", "p.zza", "vicolo", "corso",
        "largo", "salita", "lungomare",
        // Spanish / Catalan / Portuguese
        "calle", "c.", "avenida", "avda", "carrer", "plaza", "plaça",
        "paseo", "rambla", "ronda", "travesía", "travesia", "praça",
        // Dutch / Flemish
        "straat", "laan", "plein", "weg", "kade", "gracht",
        "singel", "dijk", "lei",
    ];
    for tok in tokens {
        let lower = tok.to_lowercase();
        // Strip trailing punctuation (commas etc.) — we already split on
        // whitespace but the post-split token can still carry a comma.
        let trimmed = lower.trim_matches(|c: char| !c.is_alphanumeric() && c != '.');
        if STREET_KEYWORDS.contains(&trimmed) {
            return true;
        }
    }
    false
}

/// Extract trailing city name after the number: "Kungsgatan 15 Stockholm" → "Stockholm"
fn extract_after_number(original: &str, _street: &str, number: &str) -> Option<String> {
    // Find position after the number in the original string
    if let Some(num_pos) = original.find(number) {
        let after = &original[num_pos + number.len()..];
        // After the number there might be a letter suffix ("15B" → skip "B"),
        // then whitespace, then the city name. Find the first whitespace boundary.
        let remaining = if let Some(space_pos) = after.find(char::is_whitespace) {
            after[space_pos..].trim()
        } else {
            // No whitespace after number+suffix — no trailing city
            return None;
        };
        if !remaining.is_empty() && remaining.chars().next().map(|c| c.is_alphabetic()).unwrap_or(false) {
            return Some(remaining.to_owned());
        }
    }
    None
}

/// Parse "11456 Stockholm" or "Stockholm" from the city/postcode part
fn parse_city_postcode(s: &str) -> (Option<String>, Option<String>) {
    let s = s.trim();
    let words: Vec<&str> = s.split_whitespace().collect();

    if words.is_empty() {
        return (None, None);
    }

    // Check if first word is a postcode (5 digits, possibly with space: "114 56")
    let first = words[0];
    if first.len() >= 3 && first.chars().all(|c| c.is_ascii_digit()) {
        // Looks like a postcode
        let _postcode = if words.len() > 1 && words[1].len() == 2 && words[1].chars().all(|c| c.is_ascii_digit()) {
            // "114 56" format
            let pc = format!("{} {}", first, words[1]);
            let city = if words.len() > 2 {
                Some(strip_trailing_postal_letter(&words[2..].join(" ")))
            } else { None };
            return (city, Some(pc));
        } else {
            let pc = first.to_owned();
            let city = if words.len() > 1 {
                Some(strip_trailing_postal_letter(&words[1..].join(" ")))
            } else { None };
            return (city, Some(pc));
        };
    }

    // Just a city name
    (Some(strip_trailing_postal_letter(s)), None)
}

/// Strip a trailing single-letter postal-district suffix from a city name.
/// Danish addresses use "København K", "Aarhus C", "Aalborg Ø" to denote
/// the postal district within the city; the letter has no place-name
/// signal and breaks city resolution. Generic across languages — anything
/// like "<word> <one-letter>" trims the letter.
fn strip_trailing_postal_letter(city: &str) -> String {
    let words: Vec<&str> = city.split_whitespace().collect();
    if words.len() < 2 { return city.to_owned(); }
    let last = words[words.len() - 1];
    let alphas = last.chars().filter(|c| c.is_alphabetic()).count();
    if alphas == 1 {
        words[..words.len() - 1].join(" ")
    } else {
        city.to_owned()
    }
}

/// Normalize house number: lowercase, strip ranges (take first number)
fn normalize_housenumber(s: &str) -> String {
    // Handle ranges: "15-17" → "15"
    let s = if let Some(dash_pos) = s.find('-') {
        &s[..dash_pos]
    } else {
        s
    };
    s.to_lowercase().trim().to_owned()
}

// ---------------------------------------------------------------------------
// Address index (runtime)
// ---------------------------------------------------------------------------

pub struct AddressIndex {
    fst: Map<compressed_io::MmapOrVec>,
    store: AddrStore,
    postcode_fst: Option<Map<compressed_io::MmapOrVec>>,
    postcode_data: Option<Vec<u8>>,
    /// TODO_NOMINATIM_PARITY Phase 1.1: spatial sidecar over street centroids,
    /// keyed by `street_id` (the StreetHeader array index in `addr_streets.bin`).
    /// `None` means the index was built before the sidecar landed — reverse
    /// gracefully degrades to place-only behaviour.
    addr_geohash: Option<GeohashIndex>,
}

impl AddressIndex {
    pub fn open(index_dir: &Path) -> Result<Option<Self>, HeimdallError> {
        let fst_path = index_dir.join("fst_addr.fst");
        let store_path = index_dir.join("addr_streets.bin");

        // Fall back to old format if new doesn't exist
        if !fst_path.exists() || !store_path.exists() {
            return Ok(None);
        }

        let fst = {
            let data = compressed_io::mmap_or_decompress(&fst_path)?;
            Map::new(data).map_err(HeimdallError::Fst)?
        };

        let store = match AddrStore::open(&store_path)? {
            Some(s) => s,
            None => return Ok(None),
        };

        // Optional postcode index
        let pc_fst_path = index_dir.join("fst_postcode.fst");
        let pc_data_path = index_dir.join("postcode_centroids.bin");
        let (postcode_fst, postcode_data) = if pc_fst_path.exists() && pc_data_path.exists() {
            let fst = {
                let data = compressed_io::mmap_or_decompress(&pc_fst_path)?;
                Map::new(data).map_err(HeimdallError::Fst)?
            };
            let data = compressed_io::read_maybe_compressed(&pc_data_path)?;
            (Some(fst), Some(data))
        } else {
            (None, None)
        };

        // Optional address-side spatial sidecar (Phase 1.1).
        let addr_gh_path = index_dir.join("addr_geohash_index.bin");
        let addr_geohash = if addr_gh_path.exists() {
            GeohashIndex::open(&addr_gh_path).ok()
        } else {
            None
        };

        Ok(Some(Self { fst, store, postcode_fst, postcode_data, addr_geohash }))
    }

    pub fn record_count(&self) -> usize {
        self.store.total_houses()
    }

    /// Look up an address.
    ///
    /// Strategy:
    ///   1. If municipality_id is known, try exact FST key "{street}:{muni_id}" first.
    ///   2. Fall back to scanning all "{street}:*" variants, pick closest to city_coord.
    pub fn lookup(
        &self,
        query: &AddressQuery,
        municipality_id: Option<u16>,
        city_coord: Option<Coord>,
    ) -> Vec<AddrResult> {
        use fst::{IntoStreamer, Streamer};

        // Parse the house number from the query
        let (number, suffix) = parse_query_housenumber(&query.housenumber);
        if number == 0 { return vec![]; }

        // --- Step 1: try exact muni_id key first ---
        if let Some(muni_id) = municipality_id {
            let exact_key = format!("{}:{}", query.street, muni_id);
            if let Some(street_id) = self.fst.get(exact_key.as_bytes()).map(|v| v as u32) {
                if let Some(header) = self.store.get_street(street_id) {
                    if let Some(coord) = self.store.find_house(street_id, number, suffix) {
                        let street_name = self.store.street_name(&header).to_owned();
                        let hn_str = format_housenumber(number, suffix);
                        return vec![AddrResult { street: street_name, housenumber: hn_str, coord, postcode: header.postcode }];
                    }
                }
            }
        }

        // --- Step 1.5: try city-name hash if exact muni_id failed ---
        if let Some(city) = &query.city {
            let hash_id = city_name_to_muni_id(city);
            let hash_key = format!("{}:{}", query.street, hash_id);
            if let Some(street_id) = self.fst.get(hash_key.as_bytes()).map(|v| v as u32) {
                if let Some(header) = self.store.get_street(street_id) {
                    if let Some(coord) = self.store.find_house(street_id, number, suffix) {
                        let street_name = self.store.street_name(&header).to_owned();
                        let hn_str = format_housenumber(number, suffix);
                        return vec![AddrResult { street: street_name, housenumber: hn_str, coord, postcode: header.postcode }];
                    }
                }
            }
        }

        // --- Step 2: range scan all "{street}:*" variants ---
        let prefix = format!("{}:", query.street);
        let range = fst::map::OpBuilder::new()
            .add(self.fst.range().ge(prefix.as_bytes()).lt(format!("{};", query.street).as_bytes()))
            .union();

        let mut candidates: Vec<(AddrResult, f64)> = Vec::new();

        let mut stream = range.into_stream();
        let max_candidates = 50; // Cap to avoid scanning thousands of "Main St" variants
        while let Some((_key_bytes, values)) = stream.next() {
            for &indexed in values.iter() {
                let street_id = indexed.value as u32;
                if let Some(coord) = self.store.find_house(street_id, number, suffix) {
                    // get_street is cached — find_house already decompressed this block
                    let (street_name, postcode) = self.store.get_street(street_id)
                        .map(|h| (self.store.street_name(&h).to_owned(), h.postcode))
                        .unwrap_or_default();
                    let hn_str = format_housenumber(number, suffix);

                    let dist = city_coord
                        .map(|cc| cc.distance_m(&coord))
                        .unwrap_or(0.0);

                    candidates.push((AddrResult {
                        street: street_name,
                        housenumber: hn_str,
                        coord,
                        postcode,
                    }, dist));

                    if candidates.len() >= max_candidates {
                        break;
                    }
                }
            }
            if candidates.len() >= max_candidates {
                break;
            }
        }

        if candidates.is_empty() {
            return vec![];
        }

        if city_coord.is_some() {
            candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            // Tighter threshold: 15km when we have a city reference
            if candidates[0].1 < 15_000.0 {
                return vec![candidates.remove(0).0];
            }
            return vec![];
        }

        vec![candidates.remove(0).0]
    }

    /// Look up a street by name only (no housenumber). Returns the street's
    /// representative coordinate (base_lat/base_lon from the StreetHeader).
    /// Used for "Hauptstraße, Berlin" queries where no housenumber is given.
    pub fn lookup_street(
        &self,
        street: &str,
        municipality_id: Option<u16>,
        city_coord: Option<Coord>,
    ) -> Option<AddrResult> {
        use fst::{IntoStreamer, Streamer};

        // --- Fast path: exact muni_id key ---
        if let Some(muni_id) = municipality_id {
            let exact_key = format!("{}:{}", street, muni_id);
            if let Some(street_id) = self.fst.get(exact_key.as_bytes()).map(|v| v as u32) {
                if let Some(header) = self.store.get_street(street_id) {
                    let street_name = self.store.street_name(&header).to_owned();
                    return Some(AddrResult {
                        street: street_name,
                        housenumber: String::new(),
                        coord: Coord { lat: header.base_lat, lon: header.base_lon },
                        postcode: header.postcode,
                    });
                }
            }
        }

        // --- Fallback: range scan all "{street}:*" variants, pick closest ---
        let prefix = format!("{}:", street);
        let range = fst::map::OpBuilder::new()
            .add(self.fst.range().ge(prefix.as_bytes()).lt(format!("{};", street).as_bytes()))
            .union();

        let mut candidates: Vec<(AddrResult, f64)> = Vec::new();
        let max_candidates = 50;

        let mut stream = range.into_stream();
        while let Some((_key_bytes, values)) = stream.next() {
            for &indexed in values.iter() {
                let street_id = indexed.value as u32;
                if let Some(header) = self.store.get_street(street_id) {
                    let street_name = self.store.street_name(&header).to_owned();
                    let postcode = header.postcode;
                    let coord = Coord { lat: header.base_lat, lon: header.base_lon };

                    let dist = city_coord
                        .map(|cc| cc.distance_m(&coord))
                        .unwrap_or(0.0);

                    candidates.push((AddrResult {
                        street: street_name,
                        housenumber: String::new(),
                        coord,
                        postcode,
                    }, dist));

                    if candidates.len() >= max_candidates {
                        break;
                    }
                }
            }
            if candidates.len() >= max_candidates {
                break;
            }
        }

        if candidates.is_empty() {
            return None;
        }

        // If city_coord available, return the closest street segment
        if city_coord.is_some() {
            candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            // Only return if within 15km of the city
            if candidates[0].1 < 15_000.0 {
                return Some(candidates.remove(0).0);
            }
            return None;
        }

        Some(candidates.remove(0).0)
    }

    /// Look up a postcode. Returns the centroid and display form.
    pub fn lookup_postcode(&self, query: &str) -> Option<AddrResult> {
        let fst = self.postcode_fst.as_ref()?;
        let data = self.postcode_data.as_ref()?;

        // Normalize: lowercase, strip spaces
        let norm: String = query.chars()
            .filter(|c| !c.is_whitespace())
            .flat_map(|c| c.to_lowercase())
            .collect();

        let idx = fst.get(norm.as_bytes())? as usize;

        // Read from postcode_centroids.bin
        // Format: [u32 count][entries: lat_i32, lon_i32, display_len_u8, display_bytes]
        let count = u32::from_le_bytes(data[0..4].try_into().ok()?) as usize;
        if idx >= count { return None; }

        // Walk to the idx-th entry (variable length due to display string)
        let mut pos = 4usize;
        for _ in 0..idx {
            if pos + 9 > data.len() { return None; }
            pos += 8; // skip lat + lon
            let dlen = data[pos] as usize;
            pos += 1 + dlen;
        }

        if pos + 9 > data.len() { return None; }
        let lat = i32::from_le_bytes(data[pos..pos+4].try_into().ok()?);
        let lon = i32::from_le_bytes(data[pos+4..pos+8].try_into().ok()?);
        let dlen = data[pos+8] as usize;
        let display = std::str::from_utf8(&data[pos+9..pos+9+dlen]).unwrap_or("").to_owned();

        Some(AddrResult {
            street: display,
            housenumber: String::new(),
            coord: Coord { lat, lon },
            postcode: 0,
        })
    }

    /// Reverse-geocode against the address-side spatial index.
    /// Returns `None` if the sidecar is missing (old indices), if no street
    /// is in the 3×3 cell, or if the nearest street's nearest house is
    /// further than `max_distance_m`.
    ///
    /// Scoring: walk every house of every candidate street and pick the
    /// globally-closest house. The street centroid only places the candidate
    /// in the right cell — distance is always measured house-to-query, so
    /// long streets whose centroid sits at one end still match correctly
    /// when the query is near the other end.
    ///
    /// Performance: dense urban 3×3 cells hold up to a few hundred streets
    /// with a few dozen houses each — a few thousand haversine computations
    /// per query, well under 10 ms. Sparse rural cells contain little.
    pub fn nearest_address(&self, lat: f64, lon: f64, max_distance_m: f64) -> Option<AddrResult> {
        let gh = self.addr_geohash.as_ref()?;
        let candidates = gh.nearest_raw(lat, lon);
        if candidates.is_empty() { return None; }
        let query = Coord::new(lat, lon);

        let mut best: Option<(u32, HouseEntry, f64)> = None;
        for street_id in candidates {
            let header = match self.store.get_street(street_id) {
                Some(h) => h,
                None => continue,
            };
            if header.house_count == 0 { continue; }
            let houses = self.store.street_houses(&header);
            for h in houses {
                let coord = Coord {
                    lat: header.base_lat + h.delta_lat as i32,
                    lon: header.base_lon + h.delta_lon as i32,
                };
                let d = query.distance_m(&coord);
                if d > max_distance_m { continue; }
                if best.as_ref().map_or(true, |(_, _, bd)| d < *bd) {
                    best = Some((street_id, h, d));
                }
            }
        }

        let (street_id, house, _) = best?;
        let header = self.store.get_street(street_id)?;
        Some(AddrResult {
            street: self.store.street_name(&header).to_owned(),
            housenumber: format_housenumber(house.number, house.suffix),
            coord: Coord {
                lat: header.base_lat + house.delta_lat as i32,
                lon: header.base_lon + house.delta_lon as i32,
            },
            postcode: header.postcode,
        })
    }
}

/// Detect if a query looks like a UK postcode (e.g. "SW1A 2AA", "E1 6AN", "M1 1AA").
/// Returns true for patterns matching: A[A]9[9A] 9AA
pub fn is_uk_postcode(s: &str) -> bool {
    let s = s.trim().to_uppercase();
    if !s.is_ascii() { return false; }
    let clean: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    if clean.len() < 5 || clean.len() > 7 { return false; }

    // Outward: 2-4 chars (letter(s) + digit(s) + optional letter)
    // Inward: always 3 chars (digit + 2 letters)
    let inward = &clean[clean.len()-3..];
    let outward = &clean[..clean.len()-3];

    // Inward must be: digit, letter, letter
    let ib = inward.as_bytes();
    if !ib[0].is_ascii_digit() || !ib[1].is_ascii_alphabetic() || !ib[2].is_ascii_alphabetic() {
        return false;
    }

    // Outward must start with 1-2 letters, then 1-2 digits/alphanum
    let ob = outward.as_bytes();
    if ob.is_empty() || !ob[0].is_ascii_alphabetic() { return false; }
    let alpha_end = ob.iter().position(|b| !b.is_ascii_alphabetic()).unwrap_or(ob.len());
    if alpha_end == 0 || alpha_end > 2 { return false; }
    // Rest must contain at least one digit
    ob[alpha_end..].iter().any(|b| b.is_ascii_digit())
}

/// Try to parse "street city" without a comma by testing if trailing words
/// match a known place. Tries splitting from the rightmost word leftward.
///
/// The `is_place` callback should return true if the given lowercased string
/// is a known place name (city/town/village) in the index.
///
/// Examples:
///   "kungsgatan stockholm"  → Some(("kungsgatan", "stockholm"))
///   "karl johans gate oslo" → Some(("karl johans gate", "oslo"))
///   "stockholm"             → None (single word, no street part)
///   "drottninggatan 88"     → None (has a number — should be parsed as address)
pub fn parse_street_city_freeform<F>(input: &str, is_place: F) -> Option<(String, String)>
where
    F: Fn(&str) -> bool,
{
    let input = input.trim();
    let words: Vec<&str> = input.split_whitespace().collect();
    if words.len() < 2 { return None; }

    // Don't match if it looks like an address (word starts with digit)
    for w in &words[1..] {
        if w.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) {
            return None;
        }
    }

    // Try splitting from right: 1 word as city, then 2 words, up to len-1
    // (street must be at least 1 word)
    let max_city_words = (words.len() - 1).min(3); // cities rarely exceed 3 words
    for city_len in 1..=max_city_words {
        let split = words.len() - city_len;
        let city_part: String = words[split..].join(" ").to_lowercase();
        if is_place(&city_part) {
            let street_part = words[..split].join(" ").to_lowercase();
            if street_part.len() >= 2 {
                return Some((street_part, words[split..].join(" ")));
            }
        }
    }

    None
}

/// Hash a city name to a pseudo-municipality ID (u16).
/// Same hash as pack_addr uses — must stay in sync.
pub fn city_name_to_muni_id(city: &str) -> u16 {
    let lower = city.to_lowercase();
    let mut hash: u32 = 5381;
    for b in lower.bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(b as u32);
    }
    (hash % 65534) as u16 + 1
}

fn format_housenumber(number: u16, suffix: u8) -> String {
    if suffix > 0 {
        format!("{}{}", number, (b'A' + suffix - 1) as char)
    } else {
        format!("{}", number)
    }
}

fn parse_query_housenumber(s: &str) -> (u16, u8) {
    let s = s.trim();
    let num_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    if num_end == 0 { return (0, 0); }
    let number: u16 = s[..num_end].parse().unwrap_or(0);
    let suffix = if num_end < s.len() {
        let c = s.as_bytes()[num_end];
        if c.is_ascii_alphabetic() { c.to_ascii_uppercase() - b'A' + 1 } else { 0 }
    } else { 0 };
    (number, suffix)
}

#[derive(Debug, Clone)]
pub struct AddrResult {
    pub street: String,
    pub housenumber: String,
    pub coord: Coord,
    /// Postcode from the street header (0 = unknown)
    pub postcode: u16,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_address_basic() {
        let q = parse_address_query("Kungsgatan 15, Stockholm").unwrap();
        assert_eq!(q.street, "kungsgatan");
        assert_eq!(q.housenumber, "15");
        assert_eq!(q.city, Some("Stockholm".to_owned()));
    }

    #[test]
    fn test_parse_address_no_comma() {
        let q = parse_address_query("Kungsgatan 15 Stockholm").unwrap();
        assert_eq!(q.street, "kungsgatan");
        assert_eq!(q.housenumber, "15");
        // City extraction from trailing words
        assert!(q.city.is_some());
    }

    #[test]
    fn test_parse_address_with_suffix() {
        let q = parse_address_query("Kungsgatan 15B").unwrap();
        assert_eq!(q.street, "kungsgatan");
        assert_eq!(q.housenumber, "15b");
    }

    #[test]
    fn test_parse_address_range() {
        let q = parse_address_query("Kungsgatan 15-17, Stockholm").unwrap();
        assert_eq!(q.housenumber, "15");
    }

    #[test]
    fn test_parse_not_address() {
        assert!(parse_address_query("Stockholm").is_none());
        assert!(parse_address_query("Göteborg").is_none());
    }

    #[test]
    fn test_parse_with_postcode() {
        let q = parse_address_query("Kungsgatan 15, 11456 Stockholm").unwrap();
        assert_eq!(q.street, "kungsgatan");
        assert_eq!(q.housenumber, "15");
        assert_eq!(q.postcode, Some("11456".to_owned()));
        assert_eq!(q.city, Some("Stockholm".to_owned()));
    }

    // ── Number-first parsing (French / Italian / Spanish) ───────────────

    #[test]
    fn test_parse_french_address() {
        let q = parse_address_query("10 Rue de Rivoli, Paris").unwrap();
        assert_eq!(q.street, "rue de rivoli");
        assert_eq!(q.housenumber, "10");
        assert_eq!(q.city, Some("Paris".to_owned()));
    }

    #[test]
    fn test_parse_french_avenue() {
        let q = parse_address_query("1 Avenue des Champs-Élysées, Paris").unwrap();
        assert_eq!(q.street, "avenue des champs-élysées");
        assert_eq!(q.housenumber, "1");
        assert_eq!(q.city, Some("Paris".to_owned()));
    }

    #[test]
    fn test_parse_french_boulevard_abbrev() {
        // "Bd" is in our keyword list — we expect this to parse as an
        // address even though the suffix is abbreviated. Normalisation
        // happens later via the per-country abbreviations map.
        let q = parse_address_query("1 Bd de la Madeleine, Paris").unwrap();
        assert_eq!(q.street, "bd de la madeleine");
        assert_eq!(q.housenumber, "1");
    }

    #[test]
    fn test_parse_italian_address() {
        let q = parse_address_query("12 Via Roma, Milano").unwrap();
        assert_eq!(q.street, "via roma");
        assert_eq!(q.housenumber, "12");
    }

    #[test]
    fn test_parse_spanish_address() {
        let q = parse_address_query("5 Calle Mayor, Madrid").unwrap();
        assert_eq!(q.street, "calle mayor");
        assert_eq!(q.housenumber, "5");
    }

    #[test]
    fn test_french_postcode_not_parsed_as_address() {
        // "75001 Paris" must NOT parse as address — "75001" is a
        // postcode (5 digits), and there's no recognisable street
        // keyword after it. Falls through to the postcode pipeline.
        assert!(parse_address_query("75001 Paris").is_none());
    }

    #[test]
    fn test_house_number_with_bis_suffix() {
        let q = parse_address_query("10bis avenue Foch, Paris").unwrap();
        assert_eq!(q.street, "avenue foch");
        assert_eq!(q.housenumber, "10bis");
    }

    #[test]
    fn test_german_address_still_works() {
        // Number-after pattern (German / Nordic / English).
        let q = parse_address_query("Friedrichstraße 100, Berlin").unwrap();
        assert_eq!(q.street, "friedrichstraße");
        assert_eq!(q.housenumber, "100");
    }

    // ── TODO_NOMINATIM_PARITY Phase 1.1: nearest_address tests ─────────────
    //
    // These tests build a minimal on-disk AddressIndex (FST + AddrStore +
    // optional geohash sidecar) in a tempdir and exercise nearest_address
    // against it. Covered behaviours:
    //   - sidecar missing → None (graceful degradation for old indices)
    //   - two streets in the same 3×3 cell → resolve the closer one
    //   - query in the middle of nowhere → None
    //   - max_distance_m cap is honoured (street present but too far → None)

    use std::path::Path;
    use crate::addr_store::AddrStoreBuilder;
    use crate::reverse::GeohashIndexBuilder;

    /// Build the bare-minimum on-disk artifacts AddressIndex::open expects.
    /// Writes addr_streets.bin, fst_addr.fst, and optionally
    /// addr_geohash_index.bin. Returns the index_dir.
    fn build_test_index(
        dir: &Path,
        streets: &[(&str, f64, f64, &[(u16, f64, f64)])], // (name, base_lat, base_lon, [(num, lat, lon)])
        with_geohash: bool,
    ) {
        let mut builder = AddrStoreBuilder::new();
        let mut centroids: Vec<(u32, f64, f64)> = Vec::new();
        let mut fst_pairs: Vec<(Vec<u8>, u64)> = Vec::new();
        for &(name, base_lat, base_lon, houses) in streets {
            let entries: Vec<(u16, u8, i32, i32)> = houses.iter()
                .map(|&(n, lat, lon)| (n, 0, (lat * 1_000_000.0) as i32, (lon * 1_000_000.0) as i32))
                .collect();
            let id = builder.add_street(
                name,
                (base_lat * 1_000_000.0) as i32,
                (base_lon * 1_000_000.0) as i32,
                0, &entries,
            );
            // Centroid = mean of house coords (matches pack_addr.rs build path)
            let n = houses.len() as f64;
            let cx_lat = houses.iter().map(|h| h.1).sum::<f64>() / n;
            let cx_lon = houses.iter().map(|h| h.2).sum::<f64>() / n;
            centroids.push((id, cx_lat, cx_lon));
            fst_pairs.push((format!("{}:0", name.to_lowercase()).into_bytes(), id as u64));
        }
        builder.write(&dir.join("addr_streets.bin")).unwrap();

        // Minimal FST: one wildcard key per street so AddressIndex::open
        // doesn't reject the dir for a missing fst_addr.fst.
        fst_pairs.sort_by(|a, b| a.0.cmp(&b.0));
        let f = std::fs::File::create(dir.join("fst_addr.fst")).unwrap();
        let mut mb = fst::MapBuilder::new(std::io::BufWriter::new(f)).unwrap();
        for (k, v) in fst_pairs { mb.insert(&k, v).unwrap(); }
        mb.finish().unwrap();

        if with_geohash {
            let mut gh = GeohashIndexBuilder::new();
            for (id, lat, lon) in &centroids {
                gh.add(*lat, *lon, *id);
            }
            gh.write(&dir.join("addr_geohash_index.bin")).unwrap();
        }
    }

    #[test]
    fn nearest_address_returns_none_when_sidecar_missing() {
        let dir = tempfile::tempdir().unwrap();
        build_test_index(
            dir.path(),
            &[("Hamngatan", 59.3325, 18.0708, &[(1, 59.3325, 18.0708)])],
            false, // no geohash sidecar
        );
        let idx = AddressIndex::open(dir.path()).unwrap().unwrap();
        assert!(idx.nearest_address(59.3325, 18.0708, 200.0).is_none(),
            "no sidecar → must return None (graceful degradation)");
    }

    #[test]
    fn nearest_address_resolves_closer_of_two_streets() {
        let dir = tempfile::tempdir().unwrap();
        // Two streets ~30m apart (lat delta 0.0003 ≈ 33m). The query is
        // exactly on Street A's house — the function must return A, not B.
        build_test_index(
            dir.path(),
            &[
                ("Streeta", 59.3320, 18.0700, &[(1, 59.3320, 18.0700)]),
                ("Streetb", 59.3323, 18.0700, &[(1, 59.3323, 18.0700)]),
            ],
            true,
        );
        let idx = AddressIndex::open(dir.path()).unwrap().unwrap();
        let r = idx.nearest_address(59.3320, 18.0700, 200.0)
            .expect("should find a street");
        assert_eq!(r.street, "Streeta");
    }

    #[test]
    fn nearest_address_respects_max_distance() {
        let dir = tempfile::tempdir().unwrap();
        // Single street 1km north of the query — outside a 200m cap.
        build_test_index(
            dir.path(),
            &[("Far Street", 59.3415, 18.0700, &[(1, 59.3415, 18.0700)])],
            true,
        );
        let idx = AddressIndex::open(dir.path()).unwrap().unwrap();
        assert!(idx.nearest_address(59.3325, 18.0700, 200.0).is_none(),
            "street ~1km away must not satisfy a 200m cap");
        // But 5km cap should reach it. Note: the 3x3 geohash cell at
        // precision 6 spans ~3.6 km horizontally, so the candidate set
        // may not include this street even at 5km. Use a closer query.
        let r = idx.nearest_address(59.3415, 18.0700, 200.0)
            .expect("on-the-house query must hit");
        assert_eq!(r.street, "Far Street");
    }

    #[test]
    fn nearest_address_picks_nearest_house_within_a_street() {
        let dir = tempfile::tempdir().unwrap();
        build_test_index(
            dir.path(),
            &[(
                "Long Street",
                59.3325, 18.0700, // base
                &[
                    (1, 59.3320, 18.0700),
                    (2, 59.3325, 18.0700),
                    (3, 59.3330, 18.0700),
                ],
            )],
            true,
        );
        let idx = AddressIndex::open(dir.path()).unwrap().unwrap();
        let r = idx.nearest_address(59.3330, 18.0700, 200.0)
            .expect("should find house 3");
        assert_eq!(r.housenumber, "3");
    }
}
