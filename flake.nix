{
  description = "lark-git-webhook: GitHub webhook to Lark App IM relay";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      packages = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
          lark-git-webhook = pkgs.buildGoModule {
            pname = "lark-git-webhook";
            version = "0.1.0";
            # The whole flake source, including the committed vendor/ directory.
            src = self;
            subPackages = [ "cmd/lark-git-webhook" ];
            # Dependencies are vendored in the repository, so no network or
            # module hash is needed after nixpkgs itself has been fetched.
            vendorHash = null;
            # Replicate the documented static build flags. buildGoModule
            # already adds -trimpath to GOFLAGS by default; force CGO off and
            # strip symbol/debug tables with -ldflags='-s -w'.
            env.CGO_ENABLED = "0";
            ldflags = [ "-s" "-w" ];
            meta = {
              description = "Linux-only, self-hosted GitHub webhook-to-Lark App IM API relay; one static Go binary with a local bbolt database";
              mainProgram = "lark-git-webhook";
              platforms = [ "x86_64-linux" "aarch64-linux" ];
            };
          };
        in
        {
          inherit lark-git-webhook;
          default = lark-git-webhook;
        });

      devShells = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.mkShell {
            packages = [ pkgs.go ];
          };
        });
    };
}
