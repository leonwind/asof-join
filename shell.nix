{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
    nativeBuildInputs = with pkgs; [
        # gcc
        clang
        clang-tools
        ninja
        cmake
    ];
}
