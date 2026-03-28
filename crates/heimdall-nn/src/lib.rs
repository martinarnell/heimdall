/// heimdall-nn — Neural network fuzzy geocoder layer
///
/// Implements FuzzyGeocoder trait from heimdall-core.
/// Plugged into HeimdallIndex as a last-resort fallback
/// after FST exact, phonetic, and Levenshtein all miss.
///
/// Architecture (planned):
///   char embedding(64) → BiGRU(128) → MLP(64) → [lat, lon, confidence]
///
/// This crate is a stub. Implementation tracked in:
///   https://github.com/yourusername/heimdall/issues/2

use heimdall_core::types::{GeoQuery, GeoResult};
use heimdall_core::index::FuzzyGeocoder;

pub struct NeuralGeocoder {
    // TODO: model weights
}

impl NeuralGeocoder {
    pub fn new() -> Self {
        Self {}
    }
}

impl FuzzyGeocoder for NeuralGeocoder {
    fn query(&self, _text: &str, _context: &GeoQuery) -> Vec<GeoResult> {
        // TODO: implement
        vec![]
    }

    fn name(&self) -> &'static str {
        "neural"
    }
}
