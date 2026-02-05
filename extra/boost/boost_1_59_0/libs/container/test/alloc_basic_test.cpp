//////////////////////////////////////////////////////////////////////////////
//
// (C) Copyright Ion Gaztanaga 2007-2013. Distributed under the Boost
// Software License, Version 1.0. (See accompanying file
// LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// See http://www.boost.org/libs/container for documentation.
//
//////////////////////////////////////////////////////////////////////////////

#include <boost/container/detail/alloc_lib_auto_link.hpp>
#include <boost/container/allocator.hpp>
#include <boost/container/vector.hpp>
#include <boost/container/list.hpp>

using namespace boost::container;

bool basic_test()
{
   size_t received = 0;
   if(!boost_cont_all_deallocated())
      return false;
   void *ptr = boost_cont_alloc(50, 98, &received);
   if(boost_cont_size(ptr) != received)
      return false;
   if(boost_cont_allocated_memory() != boost_cont_chunksize(ptr))
      return false;

   if(boost_cont_all_deallocated())
      return false;

   boost_cont_grow(ptr, received + 20, received + 30, &received);

   if(boost_cont_allocated_memory() != boost_cont_chunksize(ptr))
      return false;

   if(boost_cont_size(ptr) != received)
      return false;

   if(!boost_cont_shrink(ptr, 100, 140, &received, 1))
      return false;

   if(boost_cont_allocated_memory() != boost_cont_chunksize(ptr))
      return false;

   if(!boost_cont_shrink(ptr, 0, 140, &received, 1))
      return false;

   if(boost_cont_allocated_memory() != boost_cont_chunksize(ptr))
      return false;

   if(boost_cont_shrink(ptr, 0, received/2, &received, 1))
      return false;

   if(boost_cont_allocated_memory() != boost_cont_chunksize(ptr))
      return false;

   if(boost_cont_size(ptr) != received)
      return false;

   boost_cont_free(ptr);

   boost_cont_malloc_check();
   if(!boost_cont_all_deallocated())
      return false;
   return true;
}

bool vector_test()
{
   typedef boost::container::vector<int, allocator<int> > Vector;
   if(!boost_cont_all_deallocated())
      return false;
   {
      const int NumElem = 1000;
      Vector v;
      v.resize(NumElem);
      int *orig_buf = &v[0];
      int *new_buf  = &v[0];
      while(orig_buf == new_buf){
         Vector::size_type cl = v.capacity() - v.size();
         while(cl--){
            v.push_back(0);
         }
         v.push_back(0);
         new_buf = &v[0];
      }
   }
   if(!boost_cont_all_deallocated())
      return false;
   return true;
}

bool list_test()
{
   typedef boost::container::list<int, allocator<int> > List;
   if(!boost_cont_all_deallocated())
      return false;
   {
      const int NumElem = 1000;
      List l;
      int values[NumElem];
      l.insert(l.end(), &values[0], &values[NumElem]);
   }
   if(!boost_cont_all_deallocated())
      return false;
   return true;
}

int main()
{
   if(!basic_test())
      return 1;
   if(!vector_test())
      return 1;
   if(!list_test())
      return 1;
   return 0;
}
