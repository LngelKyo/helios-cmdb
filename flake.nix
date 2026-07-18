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
        # Single PG with both age + pgvector so all 4 migrations work.
        pgWithExt = pkgs.postgresql_17.withPackages (p: [ p.age p.pgvector ]);
        rustToolchain = pkgs.rustPlatform.toolchain {
          channel = "stable";
          components = [ "rustfmt" "clippy" "rust-analyzer" ];
        };
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            rustToolchain
            pgWithExt
            sqlx-cli
            docker
            cargo-watch
            cargo-nextest
            gethostname
          ];
          shellHook = ''
            export DATABASE_URL="postgres://helios:helios@localhost:5432/helios_cmdb"
            export RUST_LOG=info
            # The pg_ctl / psql binaries live inside the withPackages wrapper;
            # expose them explicitly so `pg_ctl start` works.
            export PATH="${pgWithExt}/bin:$PATH"
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
