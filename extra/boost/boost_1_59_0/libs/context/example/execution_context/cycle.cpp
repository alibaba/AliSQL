
//          Copyright Oliver Kowalke 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include <cstdlib>
#include <iostream>

#include <boost/context/all.hpp>

namespace ctx=boost::context;

ctx::execution_context * other = nullptr;

int main() {
    ctx::execution_context ctx1( ctx::fixedsize_stack(),
               [] () mutable {
                    std::cout << "ctx1 started" << std::endl;
                    ( * other)();
                    std::cout << "ctx1 will terminate" << std::endl;
               });
    ctx::execution_context ctx2( ctx::fixedsize_stack(),
               [&ctx1] () mutable {
                    std::cout << "ctx2 started" << std::endl;
                    ctx1();
                    std::cout << "ctx2 will terminate" << std::endl;
               });
    ctx::execution_context ctx3( ctx::fixedsize_stack(),
               [&ctx1, &ctx2] () mutable {
                    std::cout << "ctx3 started" << std::endl;
                    ctx2();
                    ctx1();
                    std::cout << "ctx3 will terminate" << std::endl;
               });
    other = & ctx3;
    ctx3();

    std::cout << "main: done" << std::endl;

    return EXIT_SUCCESS;
}
