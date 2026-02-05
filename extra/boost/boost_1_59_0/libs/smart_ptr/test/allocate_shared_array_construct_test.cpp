/*
 * Copyright (c) 2012-2014 Glen Joseph Fernandes 
 * glenfe at live dot com
 *
 * Distributed under the Boost Software License, 
 * Version 1.0. (See accompanying file LICENSE_1_0.txt 
 * or copy at http://boost.org/LICENSE_1_0.txt)
 */
#include <boost/detail/lightweight_test.hpp>
#if !defined(BOOST_NO_CXX11_ALLOCATOR)
#include <boost/smart_ptr/allocate_shared_array.hpp>

template<typename T>
class creator {
public:
    typedef T value_type;

    creator() {
    }

    template<typename U>
    creator(const creator<U>&) {
    }

    T* allocate(std::size_t size) {
        void* p1 = ::operator new(size * sizeof(T));
        return static_cast<T*>(p1);
    }

    void deallocate(T* memory, std::size_t) {
        void* p1 = memory;
        ::operator delete(p1);
    }

    template<typename U>
    void construct(U* memory) {
        void* p1 = memory;
        ::new(p1) U();
    }

    template<typename U>
    void destroy(U* memory) {
        memory->~U();
    }
};

class type {
    friend class creator<type>;

public:
    static unsigned int instances;
    static const type object;

protected:
    explicit type() {
        instances++;
    }

    type(const type&) {
        instances++;
    }

    ~type() {
        instances--;
    }
};

unsigned int type::instances;
const type type::object;

int main() {
    BOOST_TEST(type::instances == 1);
    {
        boost::shared_ptr<type[]> a1 = boost::allocate_shared<type[]>(creator<void>(), 3);
        BOOST_TEST(a1.use_count() == 1);
        BOOST_TEST(a1.get() != 0);
        BOOST_TEST(type::instances == 4);
        a1.reset();
        BOOST_TEST(type::instances == 1);
    }

    BOOST_TEST(type::instances == 1);
    {
        boost::shared_ptr<type[3]> a1 = boost::allocate_shared<type[3]>(creator<void>());
        BOOST_TEST(a1.use_count() == 1);
        BOOST_TEST(a1.get() != 0);
        BOOST_TEST(type::instances == 4);
        a1.reset();
        BOOST_TEST(type::instances == 1);
    }

    BOOST_TEST(type::instances == 1);
    {
        boost::shared_ptr<type[][2]> a1 = boost::allocate_shared<type[][2]>(creator<void>(), 2);
        BOOST_TEST(a1.get() != 0);
        BOOST_TEST(a1.use_count() == 1);
        BOOST_TEST(type::instances == 5);
        a1.reset();
        BOOST_TEST(type::instances == 1);
    }

    BOOST_TEST(type::instances == 1);
    {
        boost::shared_ptr<type[2][2]> a1 = boost::allocate_shared<type[2][2]>(creator<void>());
        BOOST_TEST(a1.get() != 0);
        BOOST_TEST(a1.use_count() == 1);
        BOOST_TEST(type::instances == 5);
        a1.reset();
        BOOST_TEST(type::instances == 1);
    }

    BOOST_TEST(type::instances == 1);
    {
        boost::shared_ptr<const type[]> a1 = boost::allocate_shared<const type[]>(creator<void>(), 3);
        BOOST_TEST(a1.get() != 0);
        BOOST_TEST(a1.use_count() == 1);
        BOOST_TEST(type::instances == 4);
        a1.reset();
        BOOST_TEST(type::instances == 1);
    }

    BOOST_TEST(type::instances == 1);
    {
        boost::shared_ptr<const type[3]> a1 = boost::allocate_shared<const type[3]>(creator<void>());
        BOOST_TEST(a1.get() != 0);
        BOOST_TEST(a1.use_count() == 1);
        BOOST_TEST(type::instances == 4);
        a1.reset();
        BOOST_TEST(type::instances == 1);
    }

    BOOST_TEST(type::instances == 1);
    {
        boost::shared_ptr<const type[][2]> a1 = boost::allocate_shared<const type[][2]>(creator<void>(), 2);
        BOOST_TEST(a1.get() != 0);
        BOOST_TEST(a1.use_count() == 1);
        BOOST_TEST(type::instances == 5);
        a1.reset();
        BOOST_TEST(type::instances == 1);
    }

    BOOST_TEST(type::instances == 1);
    {
        boost::shared_ptr<const type[2][2]> a1 = boost::allocate_shared<const type[2][2]>(creator<void>());
        BOOST_TEST(a1.get() != 0);
        BOOST_TEST(a1.use_count() == 1);
        BOOST_TEST(type::instances == 5);
        a1.reset();
        BOOST_TEST(type::instances == 1);
    }

    return boost::report_errors();
}
#else

int main() {
    return 0;
}

#endif
