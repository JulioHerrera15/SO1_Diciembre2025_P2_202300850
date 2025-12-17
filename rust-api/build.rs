fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compila el archivo .proto
    tonic_build::compile_protos("../proto/ventas.proto")?;
    Ok(())
}