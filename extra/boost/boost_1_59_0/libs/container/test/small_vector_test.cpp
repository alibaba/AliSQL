//////////////////////////////////////////////////////////////////////////////
//
// (C) Copyright Ion Gaztanaga 2004-2013. Distributed under the Boost
// Software License, Version 1.0. (See accompanying file
// LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// See http://www.boost.org/libs/container for documentation.
//
//////////////////////////////////////////////////////////////////////////////
#include <boost/container/small_vector.hpp>
#include "vector_test.hpp"
#include "movable_int.hpp"
#include "propagate_allocator_test.hpp"
#include "default_init_test.hpp"
#include "../../intrusive/test/iterator_test.hpp"

#include <boost/container/allocator.hpp>
#include <boost/container/node_allocator.hpp>
#include <boost/container/adaptive_pool.hpp>

#include <iostream>

namespace boost {
namespace container {

template class small_vector<char, 0>;
template class small_vector<char, 1>;
template class small_vector<char, 2>;
template class small_vector<char, 10>;

template class small_vector<int, 0>;
template class small_vector<int, 1>;
template class small_vector<int, 2>;
template class small_vector<int, 10>;

//Explicit instantiation to detect compilation errors
template class boost::container::small_vector
   < test::movable_and_copyable_int
   , 10
   , test::simple_allocator<test::movable_and_copyable_int> >;

template class boost::container::small_vector
   < test::movable_and_copyable_int
   , 10
   , test::dummy_test_allocator<test::movable_and_copyable_int> >;

template class boost::container::small_vector
   < test::movable_and_copyable_int
   , 10
   , std::allocator<test::movable_and_copyable_int> >;

template class boost::container::small_vector
   < test::movable_and_copyable_int
   , 10
   , allocator<test::movable_and_copyable_int> >;

template class boost::container::small_vector
   < test::movable_and_copyable_int
   , 10
   , adaptive_pool<test::movable_and_copyable_int> >;

template class boost::container::small_vector
   < test::movable_and_copyable_int
   , 10
   , node_allocator<test::movable_and_copyable_int> >;

}}

struct boost_container_small_vector;

namespace boost { namespace container {   namespace test {

template<>
struct alloc_propagate_base<boost_container_small_vector>
{
   template <class T, class Allocator>
   struct apply
   {
      typedef boost::container::small_vector<T, 10, Allocator> type;
   };
};

}}}   //namespace boost::container::test

bool test_small_vector_base_test()
{
   typedef boost::container::small_vector_base<int> smb_t;
   {
      typedef boost::container::small_vector<int, 5> sm5_t;
      sm5_t sm5;
      smb_t &smb = sm5;
      smb.push_back(1);
      sm5_t sm5_copy(sm5);
      sm5_copy.push_back(1);
      if (!boost::container::test::CheckEqualContainers(sm5, smb))
         return false;
   }
   {
      typedef boost::container::small_vector<int, 5> sm7_t;
      sm7_t sm7;
      smb_t &smb = sm7;
      smb.push_back(2);
      sm7_t sm7_copy(sm7);
      sm7_copy.push_back(2);
      if (!boost::container::test::CheckEqualContainers(sm7, smb))
         return false;
   }
   return true;
}

int main()
{
   using namespace boost::container;
/*
   typedef small_vector<char, 0>::storage_test storage_test;
   std::cout << "needed_extra_storages: " << storage_test::needed_extra_storages << '\n';
   std::cout << "needed_bytes: " << storage_test::needed_bytes << '\n';
   std::cout << "header_bytes: " << storage_test::header_bytes << '\n';
   std::cout << "s_start: " << storage_test::s_start << '\n';

   //char
   std::cout << "sizeof(small_vector<char,  0>): " << sizeof(small_vector<char,  0>) << " extra: " << small_vector<char,  0>::needed_extra_storages << " internal storage: " << small_vector<char,  0>::internal_capacity() << '\n';
   std::cout << "sizeof(small_vector<char,  1>): " << sizeof(small_vector<char,  1>) << " extra: " << small_vector<char,  1>::needed_extra_storages << " internal storage: " << small_vector<char,  1>::internal_capacity() << '\n';
   std::cout << "sizeof(small_vector<char,  2>): " << sizeof(small_vector<char,  2>) << " extra: " << small_vector<char,  2>::needed_extra_storages << " internal storage: " << small_vector<char,  2>::internal_capacity() << '\n';
   std::cout << "sizeof(small_vector<char,  3>): " << sizeof(small_vector<char,  3>) << " extra: " << small_vector<char,  3>::needed_extra_storages << " internal storage: " << small_vector<char,  3>::internal_capacity() << '\n';
   std::cout << "sizeof(small_vector<char,  4>): " << sizeof(small_vector<char,  4>) << " extra: " << small_vector<char,  4>::needed_extra_storages << " internal storage: " << small_vector<char,  4>::internal_capacity() << '\n';
   std::cout << "sizeof(small_vector<char,  5>): " << sizeof(small_vector<char,  5>) << " extra: " << small_vector<char,  5>::needed_extra_storages << " internal storage: " << small_vector<char,  5>::internal_capacity() << '\n';
   std::cout << "\n";

   //short
   std::cout << "sizeof(small_vector<short,  0>): " << sizeof(small_vector<short,  0>) << " extra: " << small_vector<short,  0>::needed_extra_storages << " internal storage: " << small_vector<short,  0>::internal_capacity() << '\n';
   std::cout << "sizeof(small_vector<short,  1>): " << sizeof(small_vector<short,  1>) << " extra: " << small_vector<short,  1>::needed_extra_storages << " internal storage: " << small_vector<short,  1>::internal_capacity() << '\n';
   std::cout << "sizeof(small_vector<short,  2>): " << sizeof(small_vector<short,  2>) << " extra: " << small_vector<short,  2>::needed_extra_storages << " internal storage: " << small_vector<short,  2>::internal_capacity() << '\n';
   std::cout << "sizeof(small_vector<short,  3>): " << sizeof(small_vector<short,  3>) << " extra: " << small_vector<short,  3>::needed_extra_storages << " internal storage: " << small_vector<short,  3>::internal_capacity() << '\n';
   std::cout << "sizeof(small_vector<short,  4>): " << sizeof(small_vector<short,  4>) << " extra: " << small_vector<short,  4>::needed_extra_storages << " internal storage: " << small_vector<short,  4>::internal_capacity() << '\n';
   std::cout << "sizeof(small_vector<short,  5>): " << sizeof(small_vector<short,  5>) << " extra: " << small_vector<short,  5>::needed_extra_storages << " internal storage: " << small_vector<short,  5>::internal_capacity() << '\n';
*/
   if(test::vector_test< small_vector<int, 0> >())
      return 1;

   if(test::vector_test< small_vector<int, 2000> >())
      return 1;


   ////////////////////////////////////
   //    Default init test
   ////////////////////////////////////
   if(!test::default_init_test< vector<int, test::default_init_allocator<int> > >()){
      std::cerr << "Default init test failed" << std::endl;
      return 1;
   }

   ////////////////////////////////////
   //    Emplace testing
   ////////////////////////////////////
   const test::EmplaceOptions Options = (test::EmplaceOptions)(test::EMPLACE_BACK | test::EMPLACE_BEFORE);
   if(!boost::container::test::test_emplace< vector<test::EmplaceInt>, Options>()){
      return 1;
   }

   ////////////////////////////////////
   //    Allocator propagation testing
   ////////////////////////////////////
   if(!boost::container::test::test_propagate_allocator<boost_container_small_vector>()){
      return 1;
   }

   ////////////////////////////////////
   //    Initializer lists testing
   ////////////////////////////////////
   if(!boost::container::test::test_vector_methods_with_initializer_list_as_argument_for
      < boost::container::small_vector<int, 5> >()) {
      return 1;
   }

   ////////////////////////////////////
   //       Small vector base
   ////////////////////////////////////
   if (!test_small_vector_base_test()){
      return 1;
   }

   ////////////////////////////////////
   //    Iterator testing
   ////////////////////////////////////
   {
      typedef boost::container::small_vector<int, 0> cont_int;
      cont_int a; a.push_back(0); a.push_back(1); a.push_back(2);
      boost::intrusive::test::test_iterator_random< cont_int >(a);
      if(boost::report_errors() != 0) {
         return 1;
      }
   }

   return 0;
}
