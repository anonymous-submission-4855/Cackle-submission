fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/cackle_cache.proto")?;
    Ok(())
}
