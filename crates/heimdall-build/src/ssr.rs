/// ssr.rs — Kartverket SSR (Sentralt Stedsnavnregister) GML parser
///
/// Parses the 6.5GB GML file from Kartverket's place name registry into
/// `Vec<RawPlace>`. Uses streaming XML parsing (quick-xml) to handle the
/// large file without loading it all into memory.
///
/// SSR GML structure (simplified):
///   <wfs:FeatureCollection>
///     <wfs:member>
///       <app:Sted gml:id="...">
///         <app:stedsnummer>3387</app:stedsnummer>
///         <app:stedstatus>aktiv</app:stedstatus>
///         <app:navneobjekttype>navnegard</app:navneobjekttype>
///         <app:posisjon><gml:Point><gml:pos>59.627 5.799</gml:pos></gml:Point></app:posisjon>
///         <app:kommune>
///           <app:Kommune>
///             <app:kommunenavn>Vindafjord</app:kommunenavn>
///             <app:fylkesnavn>Rogaland</app:fylkesnavn>
///           </app:Kommune>
///         </app:kommune>
///         <app:stedsnavn>
///           <app:Stedsnavn>
///             <app:skrivemåte>
///               <app:Skrivemåte>
///                 <app:langnavn>Hamre</app:langnavn>
///                 <app:prioritertSkrivemåte>true</app:prioritertSkrivemåte>
///                 <app:skrivemåtestatus>godkjent</app:skrivemåtestatus>
///               </app:Skrivemåte>
///             </app:skrivemåte>
///           </app:Stedsnavn>
///         </app:stedsnavn>
///       </app:Sted>
///     </wfs:member>
///   </wfs:FeatureCollection>
///
/// Coordinates are EPSG:4258 (ETRS89 geographic), effectively WGS84.

use std::io::BufReader;
use std::path::Path;

use anyhow::{Context, Result};
use heimdall_core::types::*;
use quick_xml::events::Event;
use quick_xml::Reader;
use tracing::info;

/// A single spelling variant within an SSR place entry.
#[derive(Debug, Default)]
struct Spelling {
    name: String,
    preferred: bool,
    status: String,
}

/// Intermediate representation of one `app:Sted` element.
#[derive(Debug, Default)]
struct SsrPlace {
    stedsnummer: i64,
    status: String,
    navneobjekttype: String,
    lat: f64,
    lon: f64,
    has_coord: bool,
    kommunenavn: String,
    fylkesnavn: String,
    spellings: Vec<Spelling>,
    importance: String,
}

/// Map SSR `navneobjekttype` to our `PlaceType`.
fn map_place_type(typ: &str) -> PlaceType {
    match typ {
        // Populated places
        "by" | "storby" => PlaceType::City,
        "tettsted" | "tettbebyggelse" => PlaceType::Town,
        "bygd" | "grend" => PlaceType::Village,
        "navnegard" | "bruk" | "eneboligFritidsbolig" | "boligfelt" | "husmannsplass" => {
            PlaceType::Hamlet
        }

        // Admin-like
        "bydel" | "delbydel" => PlaceType::Suburb,

        // Natural features
        "innsjø" | "tjern" | "vann" | "dam" | "sjø" => PlaceType::Lake,
        "elv" | "bekk" | "foss" | "stryk" => PlaceType::River,
        "fjell" | "topp" | "ås" | "fjellområde" | "egg" | "nut" | "tind" | "kamBre" => {
            PlaceType::Mountain
        }
        "skog" | "myr" | "mo" | "hei" | "vidde" | "dal" => PlaceType::Forest,
        "bukt" | "vik" | "fjord" | "sund" | "poll" => PlaceType::Bay,
        "nes" | "odde" => PlaceType::Cape,
        "øy" | "holme" | "halvøy" => PlaceType::Island,

        // Infrastructure
        "stasjon" | "holdeplass" => PlaceType::Station,
        "lufthavn" | "flyplass" => PlaceType::Airport,

        // Catch-all
        _ => PlaceType::Locality,
    }
}

/// Map SSR importance codes to a numeric score (higher = more important).
fn importance_score(code: &str) -> u32 {
    match code {
        "viktighetA" => 10_000,
        "viktighetB" => 5_000,
        "viktighetC" => 1_000,
        "viktighetD" => 500,
        "viktighetE" => 100,
        _ => 50,
    }
}

/// Parse an SSR GML file into `Vec<RawPlace>`.
///
/// Filters:
/// - Only `stedstatus = "aktiv"` places
/// - Only `skrivemåtestatus = "godkjent"` spellings
/// - Preferred spelling becomes `name`, others become `alt_names`
/// - Must have valid coordinates
pub fn read_ssr_places(gml_path: &Path) -> Result<Vec<RawPlace>> {
    let file =
        std::fs::File::open(gml_path).with_context(|| format!("open {}", gml_path.display()))?;
    let file_size = file.metadata()?.len();
    info!(
        "Parsing SSR GML: {} ({:.1} GB)...",
        gml_path.display(),
        file_size as f64 / 1_073_741_824.0
    );

    let reader = BufReader::with_capacity(8 * 1024 * 1024, file); // 8MB buffer
    let mut xml = Reader::from_reader(reader);
    xml.config_mut().trim_text(true);

    let mut buf = Vec::with_capacity(1024);
    let mut places: Vec<RawPlace> = Vec::new();

    // Parser state
    let mut in_sted = false;
    let mut in_spelling = false;
    let mut in_kommune = false;
    let mut current = SsrPlace::default();
    let mut current_spelling = Spelling::default();

    // Track which element's text we want to capture
    let mut capture_field: Option<Field> = None;

    let mut total_places = 0u64;
    let mut skipped_inactive = 0u64;
    let mut skipped_no_coord = 0u64;
    let mut skipped_no_name = 0u64;

    loop {
        match xml.read_event_into(&mut buf) {
            Ok(Event::Eof) => break,
            Ok(Event::Start(ref e)) => {
                let name_bytes = e.name().as_ref().to_vec();
                let local = local_name(&name_bytes);
                match local {
                    b"Sted" => {
                        in_sted = true;
                        current = SsrPlace::default();
                    }
                    b"Kommune" if in_sted => {
                        in_kommune = true;
                    }
                    // "Skrivemåte" — the spelling variant wrapper
                    _ if in_sted && is_skrivemaate(local) => {
                        in_spelling = true;
                        current_spelling = Spelling::default();
                    }
                    // Leaf elements we want to capture text from
                    b"stedsnummer" if in_sted && !in_spelling => {
                        capture_field = Some(Field::Stedsnummer);
                    }
                    b"stedstatus" if in_sted && !in_spelling => {
                        capture_field = Some(Field::Stedstatus);
                    }
                    b"navneobjekttype" if in_sted => {
                        capture_field = Some(Field::Navneobjekttype);
                    }
                    b"pos" if in_sted && !in_spelling => {
                        capture_field = Some(Field::Pos);
                    }
                    b"kommunenavn" if in_kommune => {
                        capture_field = Some(Field::Kommunenavn);
                    }
                    b"fylkesnavn" if in_kommune => {
                        capture_field = Some(Field::Fylkesnavn);
                    }
                    b"langnavn" if in_spelling => {
                        capture_field = Some(Field::Langnavn);
                    }
                    _ if in_spelling && is_prioritert(local) => {
                        capture_field = Some(Field::Prioritert);
                    }
                    _ if in_spelling && is_skrivemaatestatus(local) => {
                        capture_field = Some(Field::Skrivemaatestatus);
                    }
                    b"sortering1Kode" if in_sted => {
                        capture_field = Some(Field::Importance);
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(ref e)) => {
                if let Some(ref field) = capture_field {
                    let text = e.unescape().unwrap_or_default();
                    match field {
                        Field::Stedsnummer => {
                            current.stedsnummer = text.parse().unwrap_or(0);
                        }
                        Field::Stedstatus => {
                            current.status = text.to_string();
                        }
                        Field::Navneobjekttype => {
                            current.navneobjekttype = text.to_string();
                        }
                        Field::Pos => {
                            // Format: "lat lon" (EPSG:4258)
                            let parts: Vec<&str> = text.split_whitespace().collect();
                            if parts.len() == 2 {
                                if let (Ok(lat), Ok(lon)) =
                                    (parts[0].parse::<f64>(), parts[1].parse::<f64>())
                                {
                                    current.lat = lat;
                                    current.lon = lon;
                                    current.has_coord = true;
                                }
                            }
                        }
                        Field::Kommunenavn => {
                            current.kommunenavn = text.to_string();
                        }
                        Field::Fylkesnavn => {
                            current.fylkesnavn = text.to_string();
                        }
                        Field::Langnavn => {
                            current_spelling.name = text.to_string();
                        }
                        Field::Prioritert => {
                            current_spelling.preferred = text.as_ref() == "true";
                        }
                        Field::Skrivemaatestatus => {
                            current_spelling.status = text.to_string();
                        }
                        Field::Importance => {
                            current.importance = text.to_string();
                        }
                    }
                }
            }
            Ok(Event::End(ref e)) => {
                let name_bytes = e.name().as_ref().to_vec();
                let local = local_name(&name_bytes);
                capture_field = None;

                if local == b"Kommune" {
                    in_kommune = false;
                } else if is_skrivemaate(local) && in_spelling {
                    in_spelling = false;
                    if !current_spelling.name.is_empty() {
                        current.spellings.push(std::mem::take(&mut current_spelling));
                    }
                } else if local == b"Sted" && in_sted {
                    in_sted = false;
                    total_places += 1;

                    if total_places % 100_000 == 0 {
                        info!(
                            "  SSR progress: {} places read, {} kept...",
                            total_places,
                            places.len()
                        );
                    }

                    // Filter inactive
                    if current.status != "aktiv" {
                        skipped_inactive += 1;
                        continue;
                    }

                    // Filter no coordinates
                    if !current.has_coord {
                        skipped_no_coord += 1;
                        continue;
                    }

                    // Filter approved spellings only
                    let approved: Vec<&Spelling> = current
                        .spellings
                        .iter()
                        .filter(|s| s.status == "godkjent" && !s.name.is_empty())
                        .collect();

                    if approved.is_empty() {
                        skipped_no_name += 1;
                        continue;
                    }

                    // Pick preferred name, fallback to first approved
                    let primary = approved
                        .iter()
                        .find(|s| s.preferred)
                        .unwrap_or(&approved[0]);

                    let alt_names: Vec<String> = approved
                        .iter()
                        .filter(|s| s.name != primary.name)
                        .map(|s| s.name.clone())
                        .collect();

                    // Use negative stedsnummer to avoid collision with OSM IDs
                    let fake_osm_id = -(current.stedsnummer.abs());

                    let place_type = map_place_type(&current.navneobjekttype);
                    let pop = match importance_score(&current.importance) {
                        s if s >= 10_000 => Some(s),
                        s if s >= 5_000 => Some(s),
                        _ => None,
                    };

                    places.push(RawPlace {
                        osm_id: fake_osm_id,
                        osm_type: OsmType::Node,
                        name: primary.name.clone(),
                        name_intl: vec![],
                        alt_names,
                        old_names: vec![],
                        coord: Coord::new(current.lat, current.lon),
                        place_type,
                        admin_level: None,
                        country_code: Some(*b"NO"),
                        admin1: if current.fylkesnavn.is_empty() {
                            None
                        } else {
                            Some(current.fylkesnavn.clone())
                        },
                        admin2: if current.kommunenavn.is_empty() {
                            None
                        } else {
                            Some(current.kommunenavn.clone())
                        },
                        population: pop,
                        wikidata: None,
                    });
                }
            }
            Err(e) => {
                tracing::warn!("XML parse error at position {}: {}", xml.buffer_position(), e);
                // Continue — some malformed entries are expected in a 6.5GB file
            }
            _ => {}
        }
        buf.clear();
    }

    info!(
        "SSR: {} places from {} entries ({} inactive, {} no-coord, {} no-name skipped)",
        places.len(),
        total_places,
        skipped_inactive,
        skipped_no_coord,
        skipped_no_name,
    );

    // Log type distribution
    let mut type_counts: std::collections::HashMap<u8, (PlaceType, usize)> =
        std::collections::HashMap::new();
    for p in &places {
        type_counts
            .entry(p.place_type as u8)
            .or_insert((p.place_type, 0))
            .1 += 1;
    }
    let mut types: Vec<_> = type_counts.into_values().collect();
    types.sort_by(|a, b| b.1.cmp(&a.1));
    for (pt, count) in types.iter().take(10) {
        info!("  {:?}: {}", pt, count);
    }

    Ok(places)
}

/// Merge SSR places into existing OSM places.
///
/// Dedup strategy: spatial proximity (50m) + name match.
/// SSR places that are close to an OSM place with the same normalized name
/// are considered duplicates. New SSR-only places are added.
pub fn merge_ssr_places(
    existing: &[RawPlace],
    ssr: &[RawPlace],
) -> Vec<RawPlace> {
    use std::collections::HashMap;

    // Build spatial index: grid cell → vec of (name_lower, index)
    // Grid cell size ~111m lat, ~55m lon at 60°N
    let cell_size = 0.001; // ~111m
    let mut grid: HashMap<(i32, i32), Vec<(String, usize)>> = HashMap::new();

    for (i, place) in existing.iter().enumerate() {
        let lat = place.coord.lat_f64();
        let lon = place.coord.lon_f64();
        let key = ((lat / cell_size) as i32, (lon / cell_size) as i32);
        grid.entry(key)
            .or_default()
            .push((place.name.to_lowercase(), i));
    }

    let mut merged = existing.to_vec();
    let mut added = 0usize;
    let mut deduped = 0usize;

    for place in ssr {
        let lat = place.coord.lat_f64();
        let lon = place.coord.lon_f64();
        let key = ((lat / cell_size) as i32, (lon / cell_size) as i32);
        let name_lower = place.name.to_lowercase();

        // Check 3x3 grid neighborhood for duplicates
        let mut is_dup = false;
        'outer: for dx in -1..=1 {
            for dy in -1..=1 {
                if let Some(entries) = grid.get(&(key.0 + dx, key.1 + dy)) {
                    for (existing_name, _idx) in entries {
                        if *existing_name == name_lower {
                            is_dup = true;
                            break 'outer;
                        }
                    }
                }
            }
        }

        if is_dup {
            deduped += 1;
        } else {
            // Add to grid so SSR entries also dedup against each other
            grid.entry(key)
                .or_default()
                .push((name_lower, merged.len()));
            merged.push(place.clone());
            added += 1;
        }
    }

    info!(
        "SSR merge: {} total ({} existing + {} new, {} deduped)",
        merged.len(),
        existing.len(),
        added,
        deduped,
    );
    merged
}

// ---- Helper types and functions ----

#[derive(Debug)]
enum Field {
    Stedsnummer,
    Stedstatus,
    Navneobjekttype,
    Pos,
    Kommunenavn,
    Fylkesnavn,
    Langnavn,
    Prioritert,
    Skrivemaatestatus,
    Importance,
}

/// Extract local name from a potentially namespace-prefixed XML tag.
/// e.g. b"app:Sted" → b"Sted", b"Sted" → b"Sted"
fn local_name(full: &[u8]) -> &[u8] {
    match full.iter().position(|&b| b == b':') {
        Some(pos) => &full[pos + 1..],
        None => full,
    }
}

/// Check if a local name is "Skrivemåte" (UTF-8 encoded).
/// The å is 0xC3 0xA5 in UTF-8.
fn is_skrivemaate(local: &[u8]) -> bool {
    // "Skrivemåte" in UTF-8: S k r i v e m å t e
    // å = [0xC3, 0xA5]
    local == "Skrivem\u{00e5}te".as_bytes()
}

/// Check if a local name is "prioritertSkrivemåte".
fn is_prioritert(local: &[u8]) -> bool {
    local == "prioritertSkrivem\u{00e5}te".as_bytes()
}

/// Check if a local name is "skrivemåtestatus".
fn is_skrivemaatestatus(local: &[u8]) -> bool {
    local == "skrivem\u{00e5}testatus".as_bytes()
}
