// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2007-2015 Barend Gehrels, Amsterdam, the Netherlands.
// Copyright (c) 2013-2015 Adam Wulkiewicz, Lodz, Poland.

// This file was modified by Oracle on 2013, 2015.
// Modifications copyright (c) 2013-2015, Oracle and/or its affiliates.

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#include "test_intersects.hpp"


#include <boost/geometry/geometries/geometries.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <boost/geometry/util/rational.hpp>


template <typename P>
void test_all()
{
    typedef bg::model::polygon<P> polygon;
    typedef bg::model::ring<P> ring;

    // intersect <=> ! disjoint (in most cases)
    // so most tests are done in disjoint test.
    // We only test compilation of a few cases.
    test_geometry<P, bg::model::box<P> >("POINT(1 1)", "BOX(0 0,2 2)", true);

    test_geometry<polygon, bg::model::box<P> >(
        "POLYGON((1992 3240,1992 1440,3792 1800,3792 3240,1992 3240))",
        "BOX(1941 2066, 2055 2166)", true);

    test_geometry<ring, bg::model::box<P> >(
        "POLYGON((1992 3240,1992 1440,3792 1800,3792 3240,1992 3240))",
        "BOX(1941 2066, 2055 2166)", true);

    test_geometry<polygon, bg::model::box<P> >(
        "POLYGON((1941 2066,2055 2066,2055 2166,1941 2166))",
        "BOX(1941 2066, 2055 2166)", true);

    test_geometry<P, bg::model::box<P> >(
        "POINT(0 0)",
        "BOX(0 0,4 4)",
        true);
}

// Those tests won't pass for rational<> because numeric_limits<> isn't specialized for this type
template <typename P>
void test_additional()
{
    test_geometry<bg::model::segment<P>, bg::model::box<P> >(
        "SEGMENT(0 0,3 3)",
        "BOX(1 2,3 5)",
        true);
    test_geometry<bg::model::segment<P>, bg::model::box<P> >(
        "SEGMENT(1 1,2 3)",
        "BOX(0 0,4 4)",
        true);
    test_geometry<bg::model::segment<P>, bg::model::box<P> >(
        "SEGMENT(1 1,1 1)",
        "BOX(1 0,3 5)",
        true);
    test_geometry<bg::model::segment<P>, bg::model::box<P> >(
        "SEGMENT(0 1,0 1)",
        "BOX(1 0,3 5)",
        false);
    test_geometry<bg::model::segment<P>, bg::model::box<P> >(
        "SEGMENT(2 1,2 1)",
        "BOX(1 0,3 5)",
        true);
    test_geometry<bg::model::linestring<P>, bg::model::box<P> >(
        "LINESTRING(0 0,1 0,10 10)",
        "BOX(1 2,3 5)",
        true);
    test_geometry<bg::model::linestring<P>, bg::model::box<P> >(
        "LINESTRING(1 2)",
        "BOX(0 0,3 5)",
        true);
}


int test_main( int , char* [] )
{
    test_all<bg::model::d2::point_xy<double> >();
    test_additional<bg::model::d2::point_xy<double> >();

#if ! defined(BOOST_GEOMETRY_RESCALE_TO_ROBUST)
    test_all<bg::model::d2::point_xy<boost::rational<int> > >();
#endif

#if defined(HAVE_TTMATH)
    test_all<bg::model::d2::point_xy<ttmath_big> >();
#endif

    return 0;
}
