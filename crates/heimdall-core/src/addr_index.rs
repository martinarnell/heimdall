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
use crate::addr_store::AddrStore;
use crate::error::HeimdallError;
use crate::compressed_io;

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

/// Split "Kungsgatan 15B" into ("Kungsgatan", "15B")
fn split_street_number(s: &str) -> Option<(String, String)> {
    let words: Vec<&str> = s.split_whitespace().collect();
    if words.len() < 2 { return None; }

    // Find the first word that starts with a digit
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
            let city = if words.len() > 2 { Some(words[2..].join(" ")) } else { None };
            return (city, Some(pc));
        } else {
            let pc = first.to_owned();
            let city = if words.len() > 1 { Some(words[1..].join(" ")) } else { None };
            return (city, Some(pc));
        };
    }

    // Just a city name
    (Some(s.to_owned()), None)
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

        Ok(Some(Self { fst, store, postcode_fst, postcode_data }))
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
}

/// Detect if a query looks like a UK postcode (e.g. "SW1A 2AA", "E1 6AN", "M1 1AA").
/// Returns true for patterns matching: A[A]9[9A] 9AA
pub fn is_uk_postcode(s: &str) -> bool {
    let s = s.trim().to_uppercase();
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
}
