/// dvv.rs — Download and parse DVV (Finnish) address data via OGC API Features
///
/// The Finnish Digital and Population Data Services Agency (DVV) publishes
/// ~3.8M building addresses through the Ryhti system's OGC API.
///
/// API endpoint:
///   https://paikkatiedot.ymparisto.fi/geoserver/ryhti_building/ogc/features/v1/collections/open_address/items
///
/// Returns GeoJSON with WGS84 coordinates by default. Key fields:
///   address_name_fin — street name (Finnish)
///   address_name_swe — street name (Swedish, bilingual areas)
///   number_part_of_address_number — house number
///   subdivision_letter_of_address_number — letter suffix
///   postal_code — 5-digit postcode
///   postal_office_fin — city (Finnish)
///   geometry.coordinates — [lon, lat] WGS84

use anyhow::Result;
use tracing::info;

use crate::extract::RawAddress;

const DVV_API_URL: &str = "https://paikkatiedot.ymparisto.fi/geoserver/ryhti_building/ogc/features/v1/collections/open_address/items";
const PAGE_SIZE: usize = 10000;

/// Download all DVV addresses via paginated OGC API requests.
/// Returns parsed RawAddress records.
pub fn download_dvv_addresses() -> Result<Vec<RawAddress>> {
    info!("Downloading DVV addresses from OGC API (page size: {})...", PAGE_SIZE);

    // First request to get total count
    let first_url = format!("{}?f=application/geo%2Bjson&limit={}&offset=0", DVV_API_URL, PAGE_SIZE);
    let first_page: serde_json::Value = ureq::get(&first_url)
        .call()
        .map_err(|e| anyhow::anyhow!("DVV API request failed: {}", e))?
        .into_json()?;

    let total = first_page["numberMatched"].as_u64().unwrap_or(0) as usize;
    info!("DVV reports {} total addresses", total);

    let mut addresses = Vec::with_capacity(total);
    let mut skipped = 0usize;

    // Parse first page
    parse_geojson_page(&first_page, &mut addresses, &mut skipped);
    info!("[1/{}] {} addresses so far...", (total + PAGE_SIZE - 1) / PAGE_SIZE, addresses.len());

    // Fetch remaining pages
    let total_pages = (total + PAGE_SIZE - 1) / PAGE_SIZE;
    for page in 1..total_pages {
        let offset = page * PAGE_SIZE;
        let url = format!("{}?f=application/geo%2Bjson&limit={}&offset={}", DVV_API_URL, PAGE_SIZE, offset);

        let response: serde_json::Value = match ureq::get(&url).call() {
            Ok(resp) => resp.into_json()?,
            Err(e) => {
                tracing::warn!("DVV API page {} failed: {}, retrying...", page, e);
                // Single retry
                std::thread::sleep(std::time::Duration::from_secs(2));
                match ureq::get(&url).call() {
                    Ok(resp) => resp.into_json()?,
                    Err(e) => {
                        tracing::warn!("DVV API page {} retry failed: {}, skipping", page, e);
                        continue;
                    }
                }
            }
        };

        parse_geojson_page(&response, &mut addresses, &mut skipped);

        if (page + 1) % 50 == 0 || page == total_pages - 1 {
            info!("[{}/{}] {} addresses so far...", page + 1, total_pages, addresses.len());
        }
    }

    info!("Downloaded {} DVV addresses ({} skipped)", addresses.len(), skipped);
    Ok(addresses)
}

/// Parse a GeoJSON FeatureCollection page into RawAddress records.
fn parse_geojson_page(
    page: &serde_json::Value,
    addresses: &mut Vec<RawAddress>,
    skipped: &mut usize,
) {
    let features = match page["features"].as_array() {
        Some(f) => f,
        None => return,
    };

    for feature in features {
        let props = &feature["properties"];
        let geom = &feature["geometry"];

        // Extract coordinates (GeoJSON: [lon, lat])
        let coords = match geom["coordinates"].as_array() {
            Some(c) if c.len() >= 2 => c,
            _ => { *skipped += 1; continue; }
        };

        let lon = match coords[0].as_f64() {
            Some(v) => v,
            None => { *skipped += 1; continue; }
        };
        let lat = match coords[1].as_f64() {
            Some(v) => v,
            None => { *skipped += 1; continue; }
        };

        // Sanity check coordinates (Finland: 59-71N, 19-32E)
        if lat < 59.0 || lat > 71.0 || lon < 19.0 || lon > 32.0 {
            *skipped += 1;
            continue;
        }

        // Street name — prefer Finnish, fall back to Swedish
        let street = props["address_name_fin"].as_str()
            .filter(|s| !s.is_empty())
            .or_else(|| props["address_name_swe"].as_str().filter(|s| !s.is_empty()));

        let street = match street {
            Some(s) => s.to_owned(),
            None => { *skipped += 1; continue; }
        };

        // House number
        let number = match props["number_part_of_address_number"].as_u64() {
            Some(n) if n > 0 => n.to_string(),
            _ => { *skipped += 1; continue; }
        };

        // Letter suffix
        let suffix = props["subdivision_letter_of_address_number"]
            .as_str()
            .filter(|s| !s.is_empty());

        let housenumber = match suffix {
            Some(s) => format!("{}{}", number, s),
            None => number,
        };

        let postcode = props["postal_code"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_owned());

        let city = props["postal_office_fin"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(|s| {
                // DVV returns city names in ALL CAPS — title-case them
                let mut chars = s.chars();
                match chars.next() {
                    Some(first) => {
                        let rest: String = chars.collect::<String>().to_lowercase();
                        format!("{}{}", first, rest)
                    }
                    None => s.to_owned(),
                }
            });

        addresses.push(RawAddress {
            osm_id: 0,
            street,
            housenumber,
            postcode,
            city,
            lat,
            lon,
        });
    }
}
