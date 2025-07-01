#ifndef SIMD_INCLUDED
#define SIMD_INCLUDED

/*
MIT License

Copyright (c) 2023 Sasha Krassovsky

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

// https://save-buffer.github.io/bloom_filter.html

/*
  Use gcc function multiversioning to optimize for a specific CPU with run-time
  detection. Works only for x86, for other architectures we provide only one
  implementation for now.
*/
#define DEFAULT_IMPLEMENTATION
#if __GNUC__ > 7
#ifdef __x86_64__
#ifdef HAVE_IMMINTRIN_H
#include <immintrin.h>
#undef DEFAULT_IMPLEMENTATION
#define DEFAULT_IMPLEMENTATION __attribute__((target("default")))
#define AVX2_IMPLEMENTATION __attribute__((target("avx2,avx,fma")))
#if __GNUC__ > 9
#define AVX512_IMPLEMENTATION __attribute__((target("avx512f,avx512bw")))
#endif
#endif
#endif
#ifdef __aarch64__
#include <arm_neon.h>
#undef DEFAULT_IMPLEMENTATION
#define NEON_IMPLEMENTATION
#endif
#endif

#endif /* SIMD_INCLUDED */
