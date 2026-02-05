/*
 * Copyright (c) 2012-2014 Glen Joseph Fernandes 
 * glenfe at live dot com
 *
 * Distributed under the Boost Software License, 
 * Version 1.0. (See accompanying file LICENSE_1_0.txt 
 * or copy at http://boost.org/LICENSE_1_0.txt)
 */
#include <boost/detail/lightweight_test.hpp>
#include <boost/smart_ptr/allocate_shared_array.hpp>

int main() {
    {
        boost::shared_ptr<int[]> a1 = boost::allocate_shared<int[]>(std::allocator<int>(), 4, 1);
        BOOST_TEST(a1[0] == 1);
        BOOST_TEST(a1[1] == 1);
        BOOST_TEST(a1[2] == 1);
        BOOST_TEST(a1[3] == 1);
    }

    {
        boost::shared_ptr<int[4]> a1 = boost::allocate_shared<int[4]>(std::allocator<int>(), 1);
        BOOST_TEST(a1[0] == 1);
        BOOST_TEST(a1[1] == 1);
        BOOST_TEST(a1[2] == 1);
        BOOST_TEST(a1[3] == 1);
    }

    {
        boost::shared_ptr<const int[]> a1 = boost::allocate_shared<const int[]>(std::allocator<int>(), 4, 1);
        BOOST_TEST(a1[0] == 1);
        BOOST_TEST(a1[1] == 1);
        BOOST_TEST(a1[2] == 1);
        BOOST_TEST(a1[3] == 1);
    }

    {
        boost::shared_ptr<const int[4]> a1 = boost::allocate_shared<const int[4]>(std::allocator<int>(), 1);
        BOOST_TEST(a1[0] == 1);
        BOOST_TEST(a1[1] == 1);
        BOOST_TEST(a1[2] == 1);
        BOOST_TEST(a1[3] == 1);
    }

    return boost::report_errors();
}
