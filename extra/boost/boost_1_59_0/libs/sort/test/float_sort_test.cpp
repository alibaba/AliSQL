//  Boost Sort library float_sort_test.cpp file  -----------------------------//

//  Copyright Steven Ross 2014. Use, modification and
//  distribution is subject to the Boost Software License, Version
//  1.0. (See accompanying file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/sort for library home page.

#include <boost/sort/spreadsort/spreadsort.hpp>
// Include unit test framework
#include <boost/test/included/test_exec_monitor.hpp>
#include <boost/test/test_tools.hpp>
#include <vector>


using namespace std;
using namespace boost::sort::spreadsort;

//Casting to an integer before bitshifting
struct rightshift {
  int operator()(const float &x, const unsigned offset) const { 
    return float_mem_cast<float, int>(x) >> offset; 
  }
};

struct rightshift_64 {
  boost::int64_t operator()(const double &x, const boost::uint64_t offset) const
  { 
    return float_mem_cast<double, boost::int64_t>(x) >> offset; 
  }
};

boost::int32_t 
rand_32(bool sign = true) {
   boost::int32_t result = rand() | (rand()<< 16);
   if (rand() % 2)
     result |= 1 << 15;
   //Adding the sign bit
   if (sign && (rand() % 2))
     result *= -1;
   return result;
}

static const unsigned input_count = 100000;

void float_test()
{
  // Prepare inputs
  vector<float> base_vec;

  //Generating semirandom numbers that will work for basic testing
  for (unsigned u = 0; u < input_count; ++u) {
    float val = float(rand_32());
    //As std::sort gives arbitrary results for NaNs and 0.0 vs. -0.0, treat all
    //those as just 0.0 for testing
    if (!(val < 0.0) && !(0.0 < val))
      base_vec.push_back(0.0);
    else
      base_vec.push_back(val);
  }
  vector<float> sorted_vec = base_vec;
  vector<float> test_vec = base_vec;
  std::sort(sorted_vec.begin(), sorted_vec.end());
  //Testing boost::sort::spreadsort version
  test_vec = base_vec;
  boost::sort::spreadsort::spreadsort(test_vec.begin(), test_vec.end());
  BOOST_CHECK(test_vec == sorted_vec);
  //One functor
  test_vec = base_vec;
  float_sort(test_vec.begin(), test_vec.end(), rightshift());
  BOOST_CHECK(test_vec == sorted_vec);
  //Both functors
  test_vec = base_vec;
  float_sort(test_vec.begin(), test_vec.end(), rightshift(), less<float>());
  BOOST_CHECK(test_vec == sorted_vec);
}

void double_test() {
  vector<double> long_base_vec;
  for (unsigned u = 0; u < input_count; ++u) {
    double val = double
    ((((boost::int64_t)rand_32()) << ((8 * sizeof(int)) -1)) + rand_32(false));
    //As std::sort gives arbitrary results for NaNs and 0.0 vs. -0.0, 
    //treat all those as just 0.0 for testing
    if (!(val < 0.0) && !(0.0 < val))
      long_base_vec.push_back(0.0);
    else
      long_base_vec.push_back(val);
  }
  vector<double> long_sorted_vec = long_base_vec;
  vector<double> long_test_vec = long_base_vec;
  float_sort(long_test_vec.begin(), long_test_vec.end());
  std::sort(long_sorted_vec.begin(), long_sorted_vec.end());
  BOOST_CHECK(long_test_vec == long_sorted_vec);
  //One functor
  long_test_vec = long_base_vec;
  float_sort(long_test_vec.begin(), long_test_vec.end(), rightshift_64());
  BOOST_CHECK(long_test_vec == long_sorted_vec);
  //Both functors
  long_test_vec = long_base_vec;
  float_sort(long_test_vec.begin(), long_test_vec.end(), rightshift_64(),
             less<double>());
  BOOST_CHECK(long_test_vec == long_sorted_vec);
}

// Verify that 0 and 1 elements work correctly.
void corner_test() {
  vector<float> test_vec;
  boost::sort::spreadsort::spreadsort(test_vec.begin(), test_vec.end());
  const float test_value = -0.0;
  test_vec.push_back(test_value);
  boost::sort::spreadsort::spreadsort(test_vec.begin(), test_vec.end());
  BOOST_CHECK(test_vec.size() == 1);
  BOOST_CHECK(test_vec[0] == test_value);
}

// test main 
int test_main( int, char*[] )
{
  srand(1);
  float_test();
  double_test();
  corner_test();    
  return 0;
}
