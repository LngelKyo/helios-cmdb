{
  description = "helios-cmdb — agent-native CMDB for the ANA fleet";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        rustToolchain = pkgs.rustPlatform.toolchain {
          channel = "stable";
          components = [ "rustfmt" "clippy" "rust-analyzer" ];
        };
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            rustToolchain
            sqlx-cli
            postgresql_17
            docker
            cargo-watch
            cargo-nextest
          ];
          shellHook = ''
            export DATABASE_URL="postgres://helios:helios@localhost:5432/helios_cmdb"
            export RUST_LOG=info
          '';
        };

        packages.default = pkgs.rustPlatform.buildRustPackage {
          pname = "helios-cmdb";
          version = "0.1.0";
          src = ./.;
          cargoLock.lockFile = ./Cargo.lock;
          doCheck = false;
          meta = with pkgs.lib; {
            description = "Agent-native CMDB for the ANA fleet";
            license = licenses.mit;
            mainProgram = "cmdb";
          };
        };
      });
}
