#ifndef VIDX_FIELD_INCLUDED
#define VIDX_FIELD_INCLUDED

/* Copyright (c) 2025, 2025, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/field.h"
#include "vidx/vidx_common.h"

namespace vidx {
class Field_vector : public Field_varstring {
 public:
  static uint32 dimension_bytes(uint32 dimensions) {
    return VECTOR_PRECISION * dimensions;
  }

  Field_vector(uchar *ptr_arg, uint32 len_arg, uint length_bytes_arg,
               uchar *null_ptr_arg, uchar null_bit_arg, uchar auto_flags_arg,
               const char *field_name_arg, TABLE_SHARE *share)
      : Field_varstring(ptr_arg, len_arg, length_bytes_arg, null_ptr_arg,
                        null_bit_arg, auto_flags_arg, field_name_arg, share,
                        &my_charset_bin) {}

  Field_vector(uint32 len_arg, bool is_nullable_arg, const char *field_name_arg,
               TABLE_SHARE *share)
      : Field_varstring(len_arg, is_nullable_arg, field_name_arg, share,
                        &my_charset_bin) {}

  Field_vector(const Field_vector &field) : Field_varstring(field) {}

  uint32 get_dimensions() const;

  void sql_type(String &res) const final {
    const CHARSET_INFO *cs = res.charset();
    size_t length = cs->cset->snprintf(
        cs, res.ptr(), res.alloced_length(),
        RDS_COMMENT_VIDX_START "vector(%u)" RDS_COMMENT_VIDX_END
                               " varbinary(%u)",
        get_dimensions(), VECTOR_PRECISION * get_dimensions());
    res.length(length);
  }
  Field_vector *clone(MEM_ROOT *mem_root) const final {
    assert(type() == MYSQL_TYPE_VARCHAR);
    return new (mem_root) Field_vector(*this);
  }
  using Field_varstring::store;
  type_conversion_status store(double nr) final;
  type_conversion_status store(longlong nr, bool unsigned_val) final;
  type_conversion_status store_decimal(const my_decimal *) final;
  type_conversion_status store(const char *from, size_t length,
                               const CHARSET_INFO *cs) final;
  uint is_equal(const Create_field *new_field) const final;
  String *val_str(String *, String *) const final;
  bool is_vector() const final { return true; }
};
}  // namespace vidx

#endif /* VIDX_FIELD_INCLUDED */
