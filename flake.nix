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
              echo "Create virtual environment with: python -m venv venv"
              echo "Activate virtual environment with: source venv/bin/activate.fish"
              echo "Start databases with: && docker compose up -d"
            '';
          };
        };
    };
}
