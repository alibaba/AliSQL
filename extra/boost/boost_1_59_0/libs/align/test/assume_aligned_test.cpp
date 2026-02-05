/*
(c) 2015 Glen Joseph Fernandes
glenjofe at gmail dot com

Distributed under the Boost Software
License, Version 1.0.
http://boost.org/LICENSE_1_0.txt
*/
#include <boost/align/assume_aligned.hpp>

void test(void* p = 0)
{
    BOOST_ALIGN_ASSUME_ALIGNED(p, 1);
    BOOST_ALIGN_ASSUME_ALIGNED(p, 2);
    BOOST_ALIGN_ASSUME_ALIGNED(p, 4);
    BOOST_ALIGN_ASSUME_ALIGNED(p, 8);
    BOOST_ALIGN_ASSUME_ALIGNED(p, 16);
    BOOST_ALIGN_ASSUME_ALIGNED(p, 32);
    BOOST_ALIGN_ASSUME_ALIGNED(p, 64);
    BOOST_ALIGN_ASSUME_ALIGNED(p, 128);
    (void)p;
}

int main()
{
    test();
    return 0;
}
