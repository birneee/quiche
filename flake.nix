{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
    self.submodules = true;
  };
  outputs =
    inputs@{
      self,
      flake-parts,
      nixpkgs,
      rust-overlay,
      crane,
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      perSystem =
        { system, ... }:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };
          craneLib = crane.mkLib pkgs;
          commonArgs = {
            src = self;
            strictDeps = true;
            nativeBuildInputs = with pkgs; [
              cmake # for boringssl
              git # for boring-sys to apply patches
              pkg-config # for qlog-dancer
              clang # for boring-sys bindgen
            ];
            buildInputs = with pkgs; [
              fontconfig # for qlog-dancer
            ];
            LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
          };
          # built once and shared across every workspace member below
          cargoArtifacts = craneLib.buildDepsOnly (
            commonArgs
            // {
              pname = "quiche-workspace-deps";
              version = "0.0.0";
              cargoExtraArgs = "--workspace";
            }
          );
          buildRustPackage =
            name: path:
            let
              manifest = (pkgs.lib.importTOML (./. + "/${path}/Cargo.toml")).package;
            in
            craneLib.buildPackage (
              commonArgs
              // {
                pname = manifest.name;
                version = manifest.version;
                inherit cargoArtifacts;
                cargoExtraArgs = "-p ${manifest.name} --bins --examples";
              }
            );
          # does not compile currently, see https://github.com/cloudflare/quiche/issues/2519
          qlogDancerWeb =
            let
              manifest = (pkgs.lib.importTOML ./qlog-dancer/Cargo.toml).package;
            in
            pkgs.rustPlatform.buildRustPackage {
              pname = "qlog-dancer-web";
              version = manifest.version;
              cargoLock = {
                lockFile = ./Cargo.lock;
                outputHashes = {
                  "wirefilter-engine-0.7.0" = "sha256-vPelFk4BBqb/YkfC8EKYp/C/clannq8iHjGYZYtOftQ=";
                };
              };
              src = self;
              nativeBuildInputs = with pkgs; [
                wasm-pack
                pkg-config
                writableTmpDirAsHomeHook
                llvmPackages.lld
              ];
              buildInputs = with pkgs; [
                fontconfig
              ];
              buildPhase = ''
                cd qlog-dancer
                wasm-pack build --target=web
              '';
              installPhase = ''
                mkdir -p $out
                cp -r pkg $out/
                cp index.html qlog-dancer.css qlog-dancer-ui.js $out/
              '';
              doCheck = false;
            };
        in
        {
          packages = {
            apps = buildRustPackage "quiche_apps" "apps";
            buffer-pool = buildRustPackage "buffer-pool" "buffer-pool";
            datagram-socket = buildRustPackage "datagram-socket" "datagram-socket";
            h3i = buildRustPackage "h3i" "h3i";
            netlog = buildRustPackage "netlog" "netlog";
            octets = buildRustPackage "octets" "octets";
            qlog = buildRustPackage "qlog" "qlog";
            qlog-dancer = buildRustPackage "qlog-dancer" "qlog-dancer";
            qlog-dancer-web = qlogDancerWeb;
            quiche = buildRustPackage "quiche" "quiche";
            task-killswitch = buildRustPackage "task-killswitch" "task-killswitch";
            tokio-quiche = buildRustPackage "tokio-quiche" "tokio-quiche";
            default = self.packages.${system}.quiche;
          };
          devShells.default =
            let
              rust-toolchain =
                with pkgs;
                pkgs.symlinkJoin {
                  name = "rust-toolchain";
                  paths = [
                    rustc
                    cargo
                    rustPlatform.rustcSrc
                  ];
                };
            in
            pkgs.mkShell {
              buildInputs = with pkgs; [
                clippy
                cmake # for boringssl
                rust-analyzer
                rust-toolchain
                (pkgs.rust-bin.nightly.latest.minimal.override { extensions = [ "rustfmt" ]; })
                pkg-config # for qlog-dancer
                fontconfig # for qlog-dancer
                clang # for boring-sys bindgen
              ];
              LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
              RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
            };
        };
    };
}
