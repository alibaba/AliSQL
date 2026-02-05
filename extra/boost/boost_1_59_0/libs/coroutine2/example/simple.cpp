
//          Copyright Oliver Kowalke 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include <cstdlib>
#include <iostream>

#include <boost/coroutine2/all.hpp>

int main()
{
    int i = 0;
    boost::coroutines2::coroutine< void >::push_type sink(
        [&]( boost::coroutines2::coroutine< void >::pull_type & source) {
            std::cout << "inside coroutine-fn" << std::endl;
        });
    sink();

    std::cout << "\nDone" << std::endl;

    return EXIT_SUCCESS;
}
