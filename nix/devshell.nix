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
      CARGO_BUILD_RUSTFLAGS = wildRustflags;

      RUST_SRC_PATH = "${rustToolchain}/lib/rustlib/src/rust/library";
      RUSTC_WRAPPER = "sccache";
    };
  };
}
