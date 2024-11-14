{ pkgs ? import <nixpkgs> {} }:
let 
    python = pkgs.python3;
    python-packages = python.withPackages (p: with p; [
        # Plotting
        matplotlib 
        seaborn
        regex
    ]);

in pkgs.mkShell {
    nativeBuildInputs = with pkgs; [
        python-packages
        texlive.combined.scheme-full
    ];
}

