//  (C) Copyright Gennadiy Rozental 2011-2015.
//  Distributed under the Boost Software License, Version 1.0.
//  (See accompanying file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/test for the library home page.
//
/// @file
/// Tests C array based dataset
// ***************************************************************************

// Boost.Test
#include <boost/test/unit_test.hpp>
#include <boost/test/data/monomorphic/array.hpp>
namespace data=boost::unit_test::data;

#include "datasets-test.hpp"

//____________________________________________________________________________//

BOOST_AUTO_TEST_CASE( test_array )
{
    int arr1[] = {1,2,3};
    BOOST_TEST( data::make( arr1 ).size() == 3 );
    double const arr2[] = {7.4,3.2};
    BOOST_TEST( data::make( arr2 ).size() == 2 );

    int arr3[] = {7,11,13,17};
    data::for_each_sample( data::make( arr3 ), check_arg_type<int>() );

    int c = 0;
    int* ptr3 = arr3;
    data::for_each_sample( data::make( arr3 ), [&c,ptr3](int i) {
        BOOST_TEST( i == ptr3[c++] );
    });

    invocation_count ic;

    ic.m_value = 0;
    data::for_each_sample( data::make( arr3 ), ic );
    BOOST_TEST( ic.m_value == 4 );

    ic.m_value = 0;
    data::for_each_sample( data::make( arr3 ), ic, 2 );
    BOOST_TEST( ic.m_value == 2 );

    ic.m_value = 0;
    data::for_each_sample( data::make( arr3 ), ic, 0 );
    BOOST_TEST( ic.m_value == 0 );

    copy_count::value() = 0;
    copy_count arr4[] = { copy_count(), copy_count() };
    data::for_each_sample( data::make( arr4 ), check_arg_type<copy_count>() );
    BOOST_TEST( copy_count::value() == 0 );

    copy_count::value() = 0;
    copy_count const arr5[] = { copy_count(), copy_count() };
    data::for_each_sample( data::make( arr5 ), check_arg_type<copy_count>() );
    BOOST_TEST( copy_count::value() == 0 );
}

//____________________________________________________________________________//

BOOST_AUTO_TEST_CASE( test_array_make_type )
{
    int arr1[] = {1,2,3};

    typedef int (&arr_t)[3];
    BOOST_STATIC_ASSERT(( boost::is_array< boost::remove_reference<arr_t>::type >::value ) );



    typedef data::result_of::make<int (&)[3]>::type dataset_array_type;
    dataset_array_type res = data::make( arr1 );
    BOOST_TEST( res.size() == 3 );


    double const arr2[] = {7.4,3.2};
    typedef data::result_of::make<double const (&)[2]>::type dataset_array_double_type;
    dataset_array_double_type res2 = data::make( arr2 );

    BOOST_TEST( res2.size() == 2 );
}

//____________________________________________________________________________//

// EOF
