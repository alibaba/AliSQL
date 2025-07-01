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

#include "sql/create_field.h"  // Create_field
#include "sql/current_thd.h"   // vidx/vidx_index.h
#include "sql/derror.h"        // ER_THD
#include "sql/my_decimal.h"    // my_decimal
#include "sql/sql_class.h"     // THD

#include "vidx/vidx_field.h"

namespace vidx {
uint32 Field_vector::get_dimensions() const {
  return get_dimensions_low(field_length, VECTOR_PRECISION);
}

type_conversion_status Field_vector::store(double) {
  my_error(ER_DATA_INCOMPATIBLE_WITH_VECTOR, MYF(0), "double", sizeof(double),
           get_dimensions());
  return TYPE_ERR_BAD_VALUE;
}

type_conversion_status Field_vector::store(longlong, bool) {
  my_error(ER_DATA_INCOMPATIBLE_WITH_VECTOR, MYF(0), "longlong",
           sizeof(longlong), get_dimensions());
  return TYPE_ERR_BAD_VALUE;
}

type_conversion_status Field_vector::store_decimal(const my_decimal *) {
  my_error(ER_DATA_INCOMPATIBLE_WITH_VECTOR, MYF(0), "decimal",
           sizeof(my_decimal), get_dimensions());
  return TYPE_ERR_BAD_VALUE;
}

type_conversion_status Field_vector::store(const char *from, size_t length,
                                           const CHARSET_INFO *cs) {
  if (cs != &my_charset_bin) {
    THD *thd = current_thd;
    ErrConvString err(from, length, cs);
    push_warning_printf(
        thd, Sql_condition::SL_WARNING, ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
        ER_THD(thd, ER_TRUNCATED_WRONG_VALUE_FOR_FIELD), "vector", err.ptr(),
        field_name, thd->get_stmt_da()->current_row_for_condition());
  }

  if (length != field_length) {
    /* Validate length should be the same as the field length. */
  wrong_length_return:
    my_error(ER_DATA_INCOMPATIBLE_WITH_VECTOR, MYF(0), "string", length,
             get_dimensions());
    return TYPE_ERR_BAD_VALUE;
  }

  uint32 dimensions = get_dimensions_low(length, VECTOR_PRECISION);
  if (dimensions == UINT_MAX32 || dimensions > get_dimensions()) {
    goto wrong_length_return;
  }

  /* Validate every dimension and abs value of the vector. */
  float abs2 = 0.0f;
  for (uint32 i = 0; i < dimensions; i++) {
    float to_store = 0;
    memcpy(&to_store, from + sizeof(float) * i, sizeof(float));
    if (std::isnan(to_store) || std::isinf(to_store)) {
      goto wrong_data_return;
    }
    float val = get_float(from + sizeof(float) * i);
    abs2 += val * val;
  }

  if (!std::isfinite(abs2)) {
  wrong_data_return:
    THD *thd = current_thd;
    ErrConvString err(from, length, cs);
    my_error(ER_TRUNCATED_WRONG_VALUE_FOR_FIELD, MYF(0), "vector", err.ptr(),
             field_name, thd->get_stmt_da()->current_row_for_condition());
    return TYPE_ERR_BAD_VALUE;
  }

#ifdef WORDS_BIGENDIAN
  if (value.alloc(length)) {
    reset();
    return TYPE_ERR_OOM;
  }
  for (uint32 i = 0; i < dimensions; i++) {
    float to_store = 0;
    memcpy(&to_store, from + sizeof(float) * i, sizeof(float));
    float4store(value.ptr() + i * sizeof(float), to_store);
  }
  from = value.ptr();
#endif

  return Field_varstring::store(from, length, cs);
}

uint Field_vector::is_equal(const Create_field *new_field) const {
  if (new_field->sql_type != type() ||
      new_field->max_display_width_in_codepoints() != field_length ||
      new_field->charset != field_charset) {
    return IS_EQUAL_NO;
  }
  return IS_EQUAL_YES;
}

String *Field_vector::val_str(String *, String *val_ptr) const {
  ASSERT_COLUMN_MARKED_FOR_READ;

  const char *data = pointer_cast<const char *>(data_ptr());
  if (data == nullptr) {
    val_ptr->set("", 0, charset());  // A bit safer than ->length(0)
  } else {
    uint32 length = data_length();
#ifdef WORDS_BIGENDIAN
    val_ptr->alloc(length);
    uint32 dimensions = get_dimensions_low(length, VECTOR_PRECISION);
    float *to_store = (float *)(val_ptr->ptr());
    for (uint32 i = 0; i < dimensions; i++) {
      to_store[i] = float4get((const uchar *)(data + i * sizeof(float)));
    }
    val_ptr->length(length);
#else
    val_ptr->set(data, length, charset());
#endif
  }
  return val_ptr;
}
}  // namespace vidx
