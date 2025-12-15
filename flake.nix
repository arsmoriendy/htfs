{
  description = "Flake for developing PreTFS";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
  };

  outputs =
    { self, nixpkgs }:
    let
      pkgs = nixpkgs.legacyPackages."x86_64-linux";
    in
    {
      devShells."x86_64-linux".default = pkgs.mkShell {
        packages = with pkgs; [
          rustup
          pkg-config
          fuse3
        ];
        # LIBRARY_PATH = "${pkgs.lib.makeLibraryPath [ pkgs.fuse3 ]}";
        # C_INCLUDE_PATH = "${pkgs.lib.makeIncludePath [
        #   pkgs.fuse3
        #   pkgs.glibc
        # ]}";
      };
    };
}
