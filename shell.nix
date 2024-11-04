{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
    nativeBuildInputs = with pkgs; [
        clang
        clang-tools

        ninja
        cmake
        gtest

        valgrind
    ];
}
