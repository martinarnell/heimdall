/// compressed_io.rs — Transparent zstd compression for index files.
///
/// At build time:
///   `compress_file(path, level)` reads a file, compresses it in-place with zstd.
///   The compressed file starts with the standard zstd frame magic (0xFD2FB528),
///   so we can detect it at load time.
///
/// At load time:
///   `read_maybe_compressed(path)` checks for the zstd magic bytes.
///   If found, decompresses to a Vec<u8>.
///   Otherwise, reads the raw file into a Vec<u8>.
///
///   `mmap_or_decompress(path)` is the preferred alternative for large files:
///   If uncompressed, mmaps the file (zero heap). If compressed, decompresses to Vec.
///
/// This means the query code operates on the same byte slices — completely unchanged.

use std::path::Path;
use memmap2::Mmap;

/// The standard zstd frame magic bytes.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Data that is either mmap'd (zero heap) or owned (decompressed from zstd).
pub enum MmapOrVec {
    Mmap(Mmap),
    Vec(Vec<u8>),
}

impl AsRef<[u8]> for MmapOrVec {
    fn as_ref(&self) -> &[u8] {
        match self {
            MmapOrVec::Mmap(m) => m.as_ref(),
            MmapOrVec::Vec(v) => v.as_ref(),
        }
    }
}

/// Mmap an uncompressed file, or decompress a zstd file into Vec.
/// Preferred over `read_maybe_compressed` for large files — zero heap for uncompressed data.
pub fn mmap_or_decompress(path: &Path) -> std::io::Result<MmapOrVec> {
    let file = std::fs::File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };

    if mmap.len() >= 4 && mmap[0..4] == ZSTD_MAGIC {
        // Compressed: must decompress into Vec
        let decompressed = zstd::decode_all(std::io::Cursor::new(mmap.as_ref()))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(MmapOrVec::Vec(decompressed))
    } else {
        // Uncompressed: keep the mmap
        Ok(MmapOrVec::Mmap(mmap))
    }
}

/// Read a file that may or may not be zstd-compressed.
/// Returns the decompressed (or raw) content as a Vec<u8>.
pub fn read_maybe_compressed(path: &Path) -> std::io::Result<Vec<u8>> {
    let raw = std::fs::read(path)?;
    if raw.len() >= 4 && raw[0..4] == ZSTD_MAGIC {
        // Decompress
        let decompressed = zstd::decode_all(std::io::Cursor::new(&raw))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(decompressed)
    } else {
        Ok(raw)
    }
}

/// Compress a file in-place using zstd at the given compression level (1-22).
/// Reads the file, compresses it, writes back to the same path.
/// Returns (original_size, compressed_size).
pub fn compress_file(path: &Path, level: i32) -> std::io::Result<(u64, u64)> {
    let original = std::fs::read(path)?;
    let original_size = original.len() as u64;

    let compressed = zstd::encode_all(std::io::Cursor::new(&original), level)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let compressed_size = compressed.len() as u64;

    // Only write compressed if it's actually smaller
    if compressed_size < original_size {
        std::fs::write(path, &compressed)?;
        Ok((original_size, compressed_size))
    } else {
        // Keep original (incompressible data)
        Ok((original_size, original_size))
    }
}

/// Compress raw bytes and write to a path.
/// Returns compressed size.
pub fn compress_and_write(path: &Path, data: &[u8], level: i32) -> std::io::Result<u64> {
    let compressed = zstd::encode_all(std::io::Cursor::new(data), level)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    if compressed.len() < data.len() {
        std::fs::write(path, &compressed)?;
        Ok(compressed.len() as u64)
    } else {
        std::fs::write(path, data)?;
        Ok(data.len() as u64)
    }
}
