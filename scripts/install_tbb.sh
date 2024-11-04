cd ../lib/oneTBB

mkdir build || true 
cd build

cmake -DCMAKE_INSTALL_PREFIX=./../../../.installed/oneTBB -DTBB_TEST=OFF ..
cmake --build .
cmake --install .

