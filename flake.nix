{
  description = "Database benchmark";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };
  outputs =
    { nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        format = pkgs.writeShellScriptBin "format" ''
          ruff check --select I --fix && ruff format
        '';
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            python313
            python313Packages.pip
            python313Packages.matplotlib
            ruff
            format
          ];
          shellHook = ''
            if [ ! -d ".venv" ]; then
              echo -e "\033[32mCreating virtual environment...\033[0m"
              python -m venv .venv
              source .venv/bin/activate
              pip install -r requirements.txt
            fi
            source .venv/bin/activate

            if ! docker compose ps --quiet 2>/dev/null | grep -q .; then
              echo -e "\033[32mStart mongo and postgres with: docker compose up -d\033[0m"
            fi
          '';
        };
      }
    );
}
