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
        echo "Enable Perf Counters:"
        ./enable_perf_counters.sh
    '';
}
