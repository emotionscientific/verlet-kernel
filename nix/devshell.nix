{...}: {
  perSystem = {
    pkgs,
    rustToolchain,
    commonArgs,
    wildRustflags,
    ...
  }: {
    devShells.default = pkgs.mkShell {
      inherit (commonArgs) buildInputs;

      nativeBuildInputs =
        commonArgs.nativeBuildInputs
        ++ [
          rustToolchain
          pkgs.lldb
          pkgs.sccache
          pkgs.cargo-nextest

          # scripts/build-console-assets.sh
          pkgs.bun
        ];

      # Same wild flags as the package build, so `cargo` here links with wild too.
      #
      # Host-target-scoped, NOT CARGO_BUILD_RUSTFLAGS: the latter applies to every
      # target, so it leaks these ELF-only flags into the wasm32 guest builds the
      # tests drive at runtime, where `-C link-self-contained=-linker` is unstable
      # and fails the build outright.
      "CARGO_TARGET_${pkgs.stdenv.hostPlatform.rust.cargoEnvVarTarget}_RUSTFLAGS" = wildRustflags;

      RUST_SRC_PATH = "${rustToolchain}/lib/rustlib/src/rust/library";
      RUSTC_WRAPPER = "sccache";
    };
  };
}
