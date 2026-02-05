/*=============================================================================
    Copyright (c) 2015 Kohei Takahashi

    Use modification and distribution are subject to the Boost Software
    License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
    http://www.boost.org/LICENSE_1_0.txt).
==============================================================================*/

#ifndef FUSION_TEST_SFINAE_FRIENDLY_HPP
#define FUSION_TEST_SFINAE_FRIENDLY_HPP

#include <boost/config.hpp>
#include <boost/mpl/bool.hpp>
#include <boost/mpl/assert.hpp>
#include <boost/fusion/support/detail/result_of.hpp>

#if !defined(BOOST_NO_SFINAE) && !defined(BOOST_FUSION_NO_DECLTYPE_BASED_RESULT_OF)

#include <boost/fusion/container/vector.hpp>

namespace sfinae_friendly
{
    template <typename, typename T = void> struct void_ { typedef T type; };

    template <typename> struct arg_;
    template <typename R, typename T> struct arg_<R(T)> { typedef T type; };

    template <typename Traits, typename = void>
    struct check
        : boost::mpl::true_ { };

    template <typename Traits>
    struct check<Traits, typename void_<typename Traits::type>::type>
        : boost::mpl::false_ { };

    struct unspecified {};
    typedef boost::fusion::vector<> v0;
    typedef boost::fusion::vector<unspecified> v1;
    typedef boost::fusion::vector<unspecified, unspecified> v2;
    typedef boost::fusion::vector<unspecified, unspecified, unspecified> v3;
}

#define SFINAE_FRIENDLY_ASSERT(Traits) \
    BOOST_MPL_ASSERT((::sfinae_friendly::check<typename ::sfinae_friendly::arg_<void Traits>::type>))

#else

#define SFINAE_FRIENDLY_ASSERT(Traits) \
    BOOST_MPL_ASSERT((boost::mpl::true_))

#endif

#endif // FUSION_TEST_SFINAE_FRIENDLY_HPP

