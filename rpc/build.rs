fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/serruf-rpc/serruf-rpc.proto")?;
    Ok(())
}
