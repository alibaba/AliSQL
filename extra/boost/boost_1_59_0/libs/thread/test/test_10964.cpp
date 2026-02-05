// Copyright (C) 2015 Vicente Botet
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#define BOOST_THREAD_VERSION 4
#include <boost/config.hpp>
#if ! defined  BOOST_NO_CXX11_DECLTYPE
#define BOOST_RESULT_OF_USE_DECLTYPE
#endif

#include <boost/thread/future.hpp>
#include <boost/static_assert.hpp>

struct TestCallback
{
  typedef boost::future<void> result_type;

  result_type operator()(boost::future<void> future) const
  {
    future.get();
    return boost::make_ready_future();
  }

  result_type operator()(boost::future<boost::future<void> > future) const
  {
    future.get();
    return boost::make_ready_future();
  }
};

void p1()
{
}

int main()
{
#if ! defined  BOOST_NO_CXX11_DECLTYPE && ! defined  BOOST_NO_CXX11_AUTO_DECLARATIONS
  std::cout << __FILE__ << "[" << __LINE__ << "]" << std::endl;
  {
    auto f1 = boost::make_ready_future().then(TestCallback());
    BOOST_STATIC_ASSERT(std::is_same<decltype(f1), boost::future<boost::future<void> > >::value);
    f1.wait();
  }
  std::cout << __FILE__ << "[" << __LINE__ << "]" << std::endl;
  {
    auto f1 = boost::make_ready_future().then(TestCallback());
    BOOST_STATIC_ASSERT(std::is_same<decltype(f1), boost::future<boost::future<void> > >::value);
    auto f2 = f1.unwrap();
    BOOST_STATIC_ASSERT(std::is_same<decltype(f2), boost::future<void> >::value);
    f2.wait();
  }
  std::cout << __FILE__ << "[" << __LINE__ << "]" << std::endl;
  {
    auto f1 = boost::make_ready_future().then(TestCallback());
    BOOST_STATIC_ASSERT(std::is_same<decltype(f1), boost::future<boost::future<void> > >::value);
    auto f2 = f1.unwrap();
    BOOST_STATIC_ASSERT(std::is_same<decltype(f2), boost::future<void> >::value);
    auto f3 = f2.then(TestCallback());
    BOOST_STATIC_ASSERT(std::is_same<decltype(f1), boost::future<boost::future<void> > >::value);
    f3.wait();
  }
  std::cout << __FILE__ << "[" << __LINE__ << "]" << std::endl;
  {
        boost::make_ready_future().then(
            TestCallback()).unwrap().then(TestCallback()).get();
  }
  std::cout << __FILE__ << "[" << __LINE__ << "]" << std::endl;
  {
    boost::future<void> f = boost::async(p1);
    f.then(
            TestCallback()).unwrap().then(TestCallback()).get();
  }
#endif
  return 0;
}
