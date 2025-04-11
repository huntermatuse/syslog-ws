fn main() {
    ::capnpc::CompilerCommand::new()
        .file("schema/syslog.capnp")
        .run()
        .unwrap();

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=schema/syslog.capnp");
}
