{
  description = "Database benchmark";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  outputs =
    { nixpkgs, ... }:
    {
      devShells.x86_64-linux =
        let
          pkgs = nixpkgs.legacyPackages.x86_64-linux;
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              python313
              python313Packages.pip
            ];
            shellHook = ''
              if [ ! -d ".venv" ]; then
                echo "Creating virtual environment..."
                python -m venv .venv
                source .venv/bin/activate
                pip install -r requirements.txt
              fi
              source .venv/bin/activate

              if ! docker compose ps --quiet 2>/dev/null | grep -q .; then
                echo "Start databases with: docker compose up -d"
              fi
            '';
          };
        };
    };
}
