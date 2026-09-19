{...}: {
  perSystem = {
    pkgs,
    lib,
    craneLib,
    config,
    wildRustflags,
    ...
  }: let
    src = lib.cleanSourceWith {
      src = ../.;
      # Crates include_str!/include_bytes! fixtures and data (model-catalog.json,
      # *.wat.tpl, lint baselines, sqlite fixtures) that crane's cargo-only filter drops.
      filter = path: type:
        (craneLib.filterCargoSources path type)
        || builtins.match ".*\\.(json|jsonl|txt|tpl|sqlite3|base64)$" path != null;
    };

    commonArgs = {
      inherit src;
      strictDeps = true;

      # wild linker for every crate (deps, bin, tests, clippy).
      CARGO_BUILD_RUSTFLAGS = wildRustflags;

      # Native deps go here; the devshell inherits both lists.
      nativeBuildInputs = with pkgs; [];
      buildInputs = with pkgs; [];
    };

    cargoArtifacts = craneLib.buildDepsOnly commonArgs;
  in {
    packages = {
      verlet = craneLib.buildPackage (
        commonArgs
        // {
          inherit cargoArtifacts;
          pname = "verlet";
          # `-p verlet` alone would also build and install the smoke/support bins.
          cargoExtraArgs = "-p verlet --bin verlet --bin verlet-mcp-server --bin verlet-acp-agent";
          # Tests need network and wasm32 guest builds the sandbox can't provide.
          doCheck = false;
          meta.mainProgram = "verlet";
        }
      );
      default = config.packages.verlet;
    };

    # `nix flake check` runs these plus treefmt (added by the treefmt-nix module).
    # No nextest check: the tests need network access and wasm32 guest builds
    # that the sandbox does not provide.
    checks = {
      clippy = craneLib.cargoClippy (
        commonArgs
        // {
          inherit cargoArtifacts;
          cargoClippyExtraArgs = "--all-targets -- --deny warnings";
        }
      );
    };

    _module.args = {inherit commonArgs;};
  };
}
