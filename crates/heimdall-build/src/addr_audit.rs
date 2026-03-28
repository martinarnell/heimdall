/// addr_audit.rs — Scan OSM PBF for address data statistics
///
/// Reports:
///   - Nodes with addr:street + addr:housenumber
///   - Nodes with addr:interpolation
///   - Percentage with addr:postcode
///   - Sample addresses

use std::path::Path;
use anyhow::Result;
use osmpbf::{ElementReader, Element};
use rand::Rng;

pub fn audit_addresses(pbf_path: &Path) -> Result<()> {
    println!("Scanning {} for address data...", pbf_path.display());
    println!();

    let mut stats = AddrStats::default();
    let mut samples: Vec<AddrSample> = Vec::new();
    let mut rng = rand::thread_rng();

    // We want ~20 random samples from potentially millions of addresses.
    // Use reservoir sampling: keep 20, replace with decreasing probability.
    let sample_size = 20usize;

    let reader = ElementReader::from_path(pbf_path)?;
    reader.for_each(|element| {
        match element {
            Element::Node(node) => {
                scan_tags(node.id(), node.lat(), node.lon(), node.tags(),
                    &mut stats, &mut samples, sample_size, &mut rng);
            }
            Element::DenseNode(node) => {
                scan_tags(node.id(), node.lat(), node.lon(), node.tags(),
                    &mut stats, &mut samples, sample_size, &mut rng);
            }
            Element::Way(way) => {
                // Ways can also have addresses (buildings with addr tags)
                let mut has_street = false;
                let mut has_housenumber = false;
                let mut has_postcode = false;
                let mut has_city = false;
                let mut has_interpolation = false;
                let mut _street = String::new();
                let mut _housenumber = String::new();
                let mut _postcode = String::new();
                let mut _city = String::new();

                for (k, v) in way.tags() {
                    match k {
                        "addr:street" => { has_street = true; _street = v.to_owned(); }
                        "addr:housenumber" => { has_housenumber = true; _housenumber = v.to_owned(); }
                        "addr:postcode" => { has_postcode = true; _postcode = v.to_owned(); }
                        "addr:city" => { has_city = true; _city = v.to_owned(); }
                        "addr:interpolation" => { has_interpolation = true; }
                        _ => {}
                    }
                }

                stats.ways_seen += 1;

                if has_street && has_housenumber {
                    stats.way_addresses += 1;
                    if has_postcode { stats.way_with_postcode += 1; }
                    if has_city { stats.way_with_city += 1; }
                }
                if has_interpolation {
                    stats.way_interpolations += 1;
                }
                if has_street || has_housenumber {
                    stats.way_any_addr += 1;
                }
            }
            _ => {}
        }
    })?;

    // Print report
    println!("====================================================");
    println!("  SWEDEN ADDRESS DATA AUDIT");
    println!("====================================================");
    println!();
    println!("Nodes:");
    println!("  Total scanned:                {:>10}", stats.nodes_seen);
    println!("  With addr:street+housenumber: {:>10}", stats.node_addresses);
    println!("  With addr:interpolation:      {:>10}", stats.node_interpolations);
    println!("  With any addr:* tag:          {:>10}", stats.node_any_addr);
    println!();
    println!("Ways (buildings etc):");
    println!("  Total scanned:                {:>10}", stats.ways_seen);
    println!("  With addr:street+housenumber: {:>10}", stats.way_addresses);
    println!("  With addr:interpolation:      {:>10}", stats.way_interpolations);
    println!("  With any addr:* tag:          {:>10}", stats.way_any_addr);
    println!();

    let total_addresses = stats.node_addresses + stats.way_addresses;
    let total_with_postcode = stats.node_with_postcode + stats.way_with_postcode;
    let total_with_city = stats.node_with_city + stats.way_with_city;
    let total_interpolations = stats.node_interpolations + stats.way_interpolations;

    println!("Combined:");
    println!("  Total addresses (street+number): {:>10}", total_addresses);
    println!("  Total interpolation ways:        {:>10}", total_interpolations);
    println!("  With addr:postcode:              {:>10} ({:.1}%)",
        total_with_postcode,
        if total_addresses > 0 { total_with_postcode as f64 / total_addresses as f64 * 100.0 } else { 0.0 }
    );
    println!("  With addr:city:                  {:>10} ({:.1}%)",
        total_with_city,
        if total_addresses > 0 { total_with_city as f64 / total_addresses as f64 * 100.0 } else { 0.0 }
    );

    // Index size projection
    // Each address entry: ~40 bytes (street_id u32 + number u16 + postcode u32 + coord 8 bytes + overhead)
    let projected_bytes = total_addresses as f64 * 40.0;
    println!();
    println!("Index size projection:");
    println!("  At ~40 bytes/address:  {:.0} MB", projected_bytes / 1e6);
    println!("  At ~60 bytes/address:  {:.0} MB", total_addresses as f64 * 60.0 / 1e6);

    // Samples
    println!();
    println!("Sample addresses ({}):", samples.len().min(sample_size));
    println!("  {:<35} {:<8} {:<8} {:<20} ({}, {})",
        "Street", "Number", "Postcode", "City", "Lat", "Lon");
    println!("  {}", "-".repeat(100));
    for s in samples.iter().take(sample_size) {
        println!("  {:<35} {:<8} {:<8} {:<20} ({:.4}, {:.4})",
            truncate(&s.street, 34),
            &s.housenumber,
            s.postcode.as_deref().unwrap_or("-"),
            s.city.as_deref().unwrap_or("-"),
            s.lat, s.lon,
        );
    }

    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max { s.to_owned() }
    else { format!("{}…", &s[..max-1]) }
}

#[derive(Default)]
struct AddrStats {
    nodes_seen: usize,
    node_addresses: usize,      // addr:street + addr:housenumber
    node_interpolations: usize,  // addr:interpolation
    node_any_addr: usize,        // any addr:* tag
    node_with_postcode: usize,
    node_with_city: usize,
    ways_seen: usize,
    way_addresses: usize,
    way_interpolations: usize,
    way_any_addr: usize,
    way_with_postcode: usize,
    way_with_city: usize,
}

struct AddrSample {
    street: String,
    housenumber: String,
    postcode: Option<String>,
    city: Option<String>,
    lat: f64,
    lon: f64,
}

fn scan_tags<'a>(
    _id: i64,
    lat: f64,
    lon: f64,
    tags: impl Iterator<Item = (&'a str, &'a str)>,
    stats: &mut AddrStats,
    samples: &mut Vec<AddrSample>,
    sample_size: usize,
    rng: &mut impl Rng,
) {
    stats.nodes_seen += 1;

    let mut has_street = false;
    let mut has_housenumber = false;
    let mut has_postcode = false;
    let mut has_city = false;
    let mut has_interpolation = false;
    let mut street = String::new();
    let mut housenumber = String::new();
    let mut postcode = None;
    let mut city = None;

    for (k, v) in tags {
        match k {
            "addr:street" => { has_street = true; street = v.to_owned(); }
            "addr:housenumber" => { has_housenumber = true; housenumber = v.to_owned(); }
            "addr:postcode" => { has_postcode = true; postcode = Some(v.to_owned()); }
            "addr:city" => { has_city = true; city = Some(v.to_owned()); }
            "addr:interpolation" => { has_interpolation = true; }
            _ => {}
        }
    }

    if has_street && has_housenumber {
        stats.node_addresses += 1;
        if has_postcode { stats.node_with_postcode += 1; }
        if has_city { stats.node_with_city += 1; }

        // Reservoir sampling
        let n = stats.node_addresses;
        if samples.len() < sample_size {
            samples.push(AddrSample { street, housenumber, postcode, city, lat, lon });
        } else {
            let j = rng.gen_range(0..n);
            if j < sample_size {
                samples[j] = AddrSample { street, housenumber, postcode, city, lat, lon };
            }
        }
    }
    if has_interpolation {
        stats.node_interpolations += 1;
    }
    if has_street || has_housenumber {
        stats.node_any_addr += 1;
    }
}
