let

  # use a pinned version of nixpkgs for reproducability
  nixpkgs-version = "22.11";
  pkgs = import
    (builtins.fetchTarball {
      # Descriptive name to make the store path easier to identify
      name = "nixpkgs-${nixpkgs-version}";
      url = "https://github.com/nixos/nixpkgs/archive/${nixpkgs-version}.tar.gz";
      # Hash obtained using `nix-prefetch-url --unpack <url>`
      sha256 = "11w3wn2yjhaa5pv20gbfbirvjq6i3m7pqrq2msf0g7cv44vijwgw";
    })
    { };
in
with pkgs;
stdenv.mkDerivation {
  name = "featran-dev-env";

  buildInputs = [
    (sbt.override { jre = jdk11; })
    jdk11
  ];
}
