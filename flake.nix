{
  description = "autotakeout development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    inputs@{ nixpkgs, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      inputPaths =
        builtins.concatStringsSep ":"
          (map (input: input.outPath) (builtins.attrValues (builtins.removeAttrs inputs [ "self" ])));
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.mkShell {
            FLAKE_INPUTS = inputPaths;

            packages =
              with pkgs;
              [
                aria2
                backblaze-b2
                curl
                docker-client
                gnutar
                restic
                rclone
                uv
              ]
              ++ pkgs.lib.optionals pkgs.stdenv.isLinux [
                fuse3
              ];
          };
        }
      );
    };
}
