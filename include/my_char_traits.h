/* Copyright (c) 2024, 2025, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef MY_CHAR_TRAITS_INCLUDED
#define MY_CHAR_TRAITS_INCLUDED

#include <cstring>

template <class CharT>
struct my_char_traits;

/*
  This is a standards-compliant, drop-in replacement for
    std::char_traits<unsigned char>
  We need this because clang libc++ is removing support for it in clang 19.
  It is not a complete implementation. Rather we implement just enough to
  compile any usage of char_traits<uchar> we have in our codebase.
 */
template <>
struct my_char_traits<unsigned char> {
  using char_type = unsigned char;
  using int_type = unsigned int;

  static void assign(char_type &c1, const char_type &c2) { c1 = c2; }

  static char_type *assign(char_type *s, std::size_t n, char_type a) {
    return static_cast<char_type *>(memset(s, a, n));
  }

  static int compare(const char_type *s1, const char_type *s2, std::size_t n) {
    return memcmp(s1, s2, n);
  }

  static char_type *move(char_type *s1, const char_type *s2, std::size_t n) {
    if (n == 0) return s1;
    return static_cast<char_type *>(memmove(s1, s2, n));
  }

  static char_type *copy(char_type *s1, const char_type *s2, std::size_t n) {
    if (n == 0) return s1;
    return static_cast<char_type *>(memcpy(s1, s2, n));
  }
};

#endif  // MY_CHAR_TRAITS_INCLUDED
