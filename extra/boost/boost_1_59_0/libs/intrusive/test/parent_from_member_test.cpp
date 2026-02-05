/////////////////////////////////////////////////////////////////////////////
//
// (C) Copyright Ion Gaztanaga  2014-2014
//
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)
//
// See http://www.boost.org/libs/intrusive for documentation.
//
/////////////////////////////////////////////////////////////////////////////
#include <boost/intrusive/parent_from_member.hpp>
#include <boost/core/lightweight_test.hpp>

struct POD
{
   int   int_;
   float float_;
}pod;

struct Derived
   : public POD
{
   int   derived_int_;
   float derived_float_;
}derived;

struct Abstract
{
   int   abstract_int_;
   float abstract_float_;
   virtual void virtual_func1() = 0;
   virtual void virtual_func2() = 0;
   virtual ~Abstract(){}
};

struct DerivedPoly
   : public Abstract
{
   int   derivedpoly_int_;
   float derivedpoly_float_;
   virtual void virtual_func1(){}
   virtual void virtual_func2(){}
   Abstract *abstract()             {  return this; }
   Abstract const *abstract() const {  return this; }
} derivedpoly;

struct MultiInheritance
   : public Derived, public DerivedPoly
{
   int   multiinheritance_int_;
   float multiinheritance_float_;
} multiinheritance;

struct Abstract2
{
   int   abstract2_int_;
   float abstract2_float_;
   virtual void virtual_func1() = 0;
   virtual void virtual_func2() = 0;
   virtual ~Abstract2(){}
};

struct DerivedPoly2
   : public Abstract2
{
   int   derivedpoly2_int_;
   float derivedpoly2_float_;
   virtual void virtual_func1(){}
   virtual void virtual_func2(){}
   Abstract2 *abstract2()             {  return this; }
   Abstract2 const *abstract2() const {  return this; }
} derivedpoly2;

struct MultiInheritance2
   : public DerivedPoly, public DerivedPoly2
{
   int   multiinheritance2_int_;
   float multiinheritance2_float_;
} multiinheritance2;

struct VirtualDerived
   : public virtual Derived
{
   int   virtualderived_int_;
   float virtualderived_float_;
   virtual void f1(){}
   virtual void f2(){}
} virtualderived;

struct VirtualMultipleDerived
   : public virtual Derived, virtual public DerivedPoly
{
   int   virtualmultiplederived_int_;
   float virtualmultiplederived_float_;
   virtual void f1(){}
   virtual void f2(){}
} virtualmultiplederived;

using namespace boost::intrusive;

int main()
{
   //POD
   BOOST_TEST(&pod == get_parent_from_member(&pod.int_,   &POD::int_));
   BOOST_TEST(&pod == get_parent_from_member(&pod.float_, &POD::float_));

   //Derived
   BOOST_TEST(&derived == get_parent_from_member(&derived.int_,   &Derived::int_));
   BOOST_TEST(&derived == get_parent_from_member(&derived.float_, &Derived::float_));
   BOOST_TEST(&derived == get_parent_from_member(&derived.derived_int_,   &Derived::derived_int_));
   BOOST_TEST(&derived == get_parent_from_member(&derived.derived_float_, &Derived::derived_float_));

   //Abstract
   BOOST_TEST(derivedpoly.abstract() == get_parent_from_member(&derivedpoly.abstract_int_,   &Abstract::abstract_int_));
   BOOST_TEST(derivedpoly.abstract() == get_parent_from_member(&derivedpoly.abstract_float_, &Abstract::abstract_float_));

   //DerivedPoly
   BOOST_TEST(&derivedpoly == get_parent_from_member(&derivedpoly.abstract_int_,   &DerivedPoly::abstract_int_));
   BOOST_TEST(&derivedpoly == get_parent_from_member(&derivedpoly.abstract_float_, &DerivedPoly::abstract_float_));
   BOOST_TEST(&derivedpoly == get_parent_from_member(&derivedpoly.derivedpoly_int_,   &DerivedPoly::derivedpoly_int_));
   BOOST_TEST(&derivedpoly == get_parent_from_member(&derivedpoly.derivedpoly_float_, &DerivedPoly::derivedpoly_float_));

   //MultiInheritance
   BOOST_TEST(multiinheritance.abstract() == get_parent_from_member(&multiinheritance.abstract_int_,   &MultiInheritance::abstract_int_));
   BOOST_TEST(multiinheritance.abstract() == get_parent_from_member(&multiinheritance.abstract_float_, &MultiInheritance::abstract_float_));
   BOOST_TEST(&multiinheritance == get_parent_from_member(&multiinheritance.derivedpoly_int_,   &MultiInheritance::derivedpoly_int_));
   BOOST_TEST(&multiinheritance == get_parent_from_member(&multiinheritance.derivedpoly_float_, &MultiInheritance::derivedpoly_float_));
   BOOST_TEST(&multiinheritance == get_parent_from_member(&multiinheritance.int_,   &MultiInheritance::int_));
   BOOST_TEST(&multiinheritance == get_parent_from_member(&multiinheritance.float_, &MultiInheritance::float_));
   BOOST_TEST(&multiinheritance == get_parent_from_member(&multiinheritance.derived_int_,   &MultiInheritance::derived_int_));
   BOOST_TEST(&multiinheritance == get_parent_from_member(&multiinheritance.derived_float_, &MultiInheritance::derived_float_));

   BOOST_TEST(multiinheritance.abstract() == get_parent_from_member(&multiinheritance.abstract_int_,   &MultiInheritance::abstract_int_));
   BOOST_TEST(multiinheritance.abstract() == get_parent_from_member(&multiinheritance.abstract_float_, &MultiInheritance::abstract_float_));
   BOOST_TEST(&multiinheritance == get_parent_from_member(&multiinheritance.derivedpoly_int_,   &MultiInheritance::derivedpoly_int_));
   BOOST_TEST(&multiinheritance == get_parent_from_member(&multiinheritance.derivedpoly_float_, &MultiInheritance::derivedpoly_float_));
   BOOST_TEST(multiinheritance2.abstract2() == get_parent_from_member(&multiinheritance2.abstract2_int_,   &MultiInheritance2::abstract2_int_));
   BOOST_TEST(multiinheritance2.abstract2() == get_parent_from_member(&multiinheritance2.abstract2_float_, &MultiInheritance2::abstract2_float_));
   BOOST_TEST(&multiinheritance2 == get_parent_from_member(&multiinheritance2.derivedpoly2_int_,   &MultiInheritance2::derivedpoly2_int_));
   BOOST_TEST(&multiinheritance2 == get_parent_from_member(&multiinheritance2.derivedpoly2_float_, &MultiInheritance2::derivedpoly2_float_));

   //MSVC pointer to member data uses RTTI info even when not crossing virtual base boundaries
   #ifdef BOOST_INTRUSIVE_MSVC_ABI_PTR_TO_MEMBER
   //No access to virtual base data {int_, float_, derived_int_, derived_float_}
   BOOST_TEST(&virtualderived == get_parent_from_member(&virtualderived.virtualderived_int_,   &VirtualDerived::virtualderived_int_));
   BOOST_TEST(&virtualderived == get_parent_from_member(&virtualderived.virtualderived_float_, &VirtualDerived::virtualderived_float_));
   BOOST_TEST(&virtualmultiplederived == get_parent_from_member(&virtualmultiplederived.virtualmultiplederived_float_, &VirtualMultipleDerived::virtualmultiplederived_float_));
   BOOST_TEST(&virtualmultiplederived == get_parent_from_member(&virtualmultiplederived.virtualmultiplederived_int_,   &VirtualMultipleDerived::virtualmultiplederived_int_));
   BOOST_TEST(&virtualmultiplederived == get_parent_from_member(&virtualmultiplederived.derivedpoly_float_, &VirtualMultipleDerived::derivedpoly_float_));
   BOOST_TEST(&virtualmultiplederived == get_parent_from_member(&virtualmultiplederived.derivedpoly_int_,   &VirtualMultipleDerived::derivedpoly_int_));
   #endif

   return boost::report_errors();
}
