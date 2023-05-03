{
  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-22.11";
  inputs.mach-nix.url = "mach-nix/3.5.0";
  inputs.poetry2nix-flake = {
    url = "github:nix-community/poetry2nix";
    inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, flake-utils, mach-nix, poetry2nix-flake }: (flake-utils.lib.eachDefaultSystem (system:
    with import nixpkgs {
      inherit system;
      overlays = [ poetry2nix-flake.overlay ];
    };

    {
      devShell = mkShell {
        buildInputs = [
          chromedriver
          (poetry2nix.mkPoetryEnv {
            python = python310;
            projectDir = ./.;
            preferWheels = true;

            overrides = [ (_: poetrySuper: {
              ibm-cos-sdk = poetrySuper.ibm-cos-sdk-core.overrideAttrs(_: super: {
                nativeBuildInputs = super.nativeBuildInputs ++ [ poetrySuper.setuptools ];
              });

              ibm-cos-sdk-core = poetrySuper.ibm-cos-sdk-core.overrideAttrs(_: super: {
                nativeBuildInputs = super.nativeBuildInputs ++ [ poetrySuper.setuptools ];
              });

              ibm-cos-sdk-s3transfer = poetrySuper.ibm-cos-sdk-s3transfer.overrideAttrs(_: super: {
                nativeBuildInputs = super.nativeBuildInputs ++ [ poetrySuper.setuptools ];
              });

              ibm-vpc = poetrySuper.ibm-cos-sdk-core.overrideAttrs(_: super: {
                nativeBuildInputs = super.nativeBuildInputs ++ [ poetrySuper.setuptools ];
              });

              ibm-cloud-sdk-core = poetrySuper.ibm-cloud-sdk-core.overrideAttrs(_: super: {
                nativeBuildInputs = super.nativeBuildInputs ++ [ poetrySuper.setuptools ];
              });

              cvxpy = python310Packages.cvxpy;

              numpy = python310Packages.numpy;
            }) ];
          })
        ];
      };
    }
  ));
}
