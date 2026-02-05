
//          Copyright Oliver Kowalke 2009.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

#include <boost/array.hpp>
#include <boost/assert.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/utility.hpp>

#include <boost/context/all.hpp>
#include <boost/context/detail/config.hpp>

namespace ctx = boost::context;

int value1 = 0;
ctx::execution_context * mctx = nullptr;

void f11() {
    value1 = 3;
    ( * mctx)();
}

void f12( int i) {
    value1 = i;
    ( * mctx)();
}

void f13( int i) {
    value1 = i;
    ( * mctx)();
}

void f15() {
    ( * mctx)();
}

struct X {
    int foo( int i) {
        value1 = i;
        ( * mctx)();
        return i;
    }
};

void test_ectx() {
    boost::context::execution_context ctx( boost::context::execution_context::current() );
    mctx = & ctx;
    value1 = 0;
    ctx::fixedsize_stack alloc;
    ctx::execution_context ectx( alloc, f11);
    ectx();
    BOOST_CHECK_EQUAL( 3, value1);
}

void test_return() {
    boost::context::execution_context ctx( boost::context::execution_context::current() );
    mctx = & ctx;
    value1 = 0;
    ctx::fixedsize_stack alloc;
    ctx::execution_context ectx( alloc, f13, 3);
    ectx();
    BOOST_CHECK_EQUAL( 3, value1);
}

void test_variadric() {
    boost::context::execution_context ctx( boost::context::execution_context::current() );
    mctx = & ctx;
    value1 = 0;
    ctx::fixedsize_stack alloc;
    ctx::execution_context ectx( alloc, f12, 5);
    ectx();
    BOOST_CHECK_EQUAL( 5, value1);
}

void test_memfn() {
    boost::context::execution_context ctx( boost::context::execution_context::current() );
    mctx = & ctx;
    value1 = 0;
    X x;
    ctx::fixedsize_stack alloc;
    ctx::execution_context ectx( alloc, & X::foo, x, 7);
    ectx();
    BOOST_CHECK_EQUAL( 7, value1);
}

void test_prealloc() {
    boost::context::execution_context ctx( boost::context::execution_context::current() );
    mctx = & ctx;
    value1 = 0;
    ctx::fixedsize_stack alloc;
    ctx::stack_context sctx( alloc.allocate() );
    void * sp = static_cast< char * >( sctx.sp) - 10;
    std::size_t size = sctx.size - 10;
    ctx::execution_context ectx( ctx::preallocated( sp, size, sctx), alloc, f12, 7);
    ectx();
    BOOST_CHECK_EQUAL( 7, value1);
}

boost::unit_test::test_suite * init_unit_test_suite( int, char* [])
{
    boost::unit_test::test_suite * test =
        BOOST_TEST_SUITE("Boost.Context: execution_context test suite");

    test->add( BOOST_TEST_CASE( & test_ectx) );
    test->add( BOOST_TEST_CASE( & test_return) );
    test->add( BOOST_TEST_CASE( & test_variadric) );
    test->add( BOOST_TEST_CASE( & test_memfn) );
    test->add( BOOST_TEST_CASE( & test_prealloc) );

    return test;
}
