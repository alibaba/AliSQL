//  (C) Copyright Raffi Enficiaud 2014.
//  Distributed under the Boost Software License, Version 1.0.
//  (See accompanying file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/test for the library home page.

//[example_code
#define BOOST_TEST_MODULE dataset_example68
#include <boost/test/included/unit_test.hpp>
#include <boost/test/data/test_case.hpp>
#include <boost/test/data/monomorphic.hpp>
#include <sstream>

namespace bdata = boost::unit_test::data;

// Dataset generating a Fibonacci sequence
class fibonacci_dataset : public bdata::monomorphic::dataset<int>
{
public:
  // Samples type is int
  typedef int data_type;
  enum { arity = 1 };

private:
  typedef bdata::monomorphic::dataset<int> base;
  
  struct iterator : base::iterator
  {
    typedef bdata::monomorphic::traits<int>::ref_type ref_type;
    int a;
    int b; // b is the output
    
    iterator() : a(0), b(0) 
    {}
    
    ref_type operator*()
    {
      return b;
    }
    
    virtual void operator++()
    {
      a = a + b;
      std::swap(a, b);
      
      if(!b)
        b = 1;
    }
  };
  
public:
  fibonacci_dataset()
  {}
  
  // size is infinite
  bdata::size_t size() const
  {
    return bdata::BOOST_TEST_DS_INFINITE_SIZE;
  }
  
  // iterator
  virtual iter_ptr begin() const
  { 
    return boost::make_shared<iterator>(); 
  }
};

namespace boost { namespace unit_test { namespace data { namespace monomorphic {
  // registering fibonacci_dataset as a proper dataset
  template <>
  struct is_dataset<fibonacci_dataset> : boost::mpl::true_ {};
}}}}

// Creating a test-driven dataset 
BOOST_DATA_TEST_CASE( 
  test1, 
  fibonacci_dataset() ^ bdata::xrange(10),
  fib_sample, index)
{
  std::cout << "test 1: " 
    << fib_sample 
    << " / index: " << index
    << std::endl;
  BOOST_TEST(fib_sample <= 13);
}
//]
