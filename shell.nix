{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
    nativeBuildInputs = with pkgs; [
        clang
        clang-tools

        ninja
        cmake
        gtest
        fmt
        tbb

        valgrind
    ];


    shellHook = ''
        ./enable_perf_counters.sh
    '';
}
