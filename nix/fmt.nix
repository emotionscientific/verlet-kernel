{...}: {
  perSystem = {pkgs, ...}: {
    treefmt.config = {
      projectRootFile = "flake.nix";

      programs = {
        # treefmt-nix defaults rustfmt to --edition 2024, which matches the
        # workspace. The agent-tools/tool-* crates are still edition 2021 and
        # CI gates on `cargo fmt`, which reads each crate's edition, so they get
        # their own rustfmt entry below.
        rustfmt = {
          enable = true;
          excludes = ["agent-tools/tool-*/*"];
        };
        alejandra.enable = true;
        taplo.enable = true;
      };

      settings.formatter.rustfmt-2021 = {
        command = "${pkgs.rustfmt}/bin/rustfmt";
        options = ["--edition" "2021"];
        includes = ["agent-tools/tool-*/*.rs"];
      };
    };
  };
}
