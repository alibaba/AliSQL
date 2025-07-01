#ifndef VIDX_FUNC_INCLUDED
#define VIDX_FUNC_INCLUDED

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

#include "vidx/vidx_common.h"

class ORDER;

namespace vidx {
enum distance_kind { EUCLIDEAN, COSINE, AUTO };

class Item_func_vec_distance : public Item_real_func {
 public:
  Item_func_vec_distance(const POS &pos, Item *a, Item *b)
      : Item_real_func(pos, a, b), kind(AUTO) {}

  Item_func_vec_distance(const POS &pos, Item *a, Item *b, distance_kind c)
      : Item_real_func(pos, a, b), kind(c) {}

  const char *func_name() const override {
    static LEX_CSTRING name[3] = {{STRING_WITH_LEN("VEC_DISTANCE_EUCLIDEAN")},
                                  {STRING_WITH_LEN("VEC_DISTANCE_COSINE")},
                                  {STRING_WITH_LEN("VEC_DISTANCE")}};
    return name[kind].str;
  }

  bool resolve_type(THD *thd) override;
  int get_key();
  double val_real() override;
  enum Functype functype() const override { return VECTOR_DISTANCE_FUNC; }
  ha_rows get_limit() const { return m_limit; }
  void set_limit(const ha_rows &limit) { m_limit = limit; }
  Item *get_const_arg() const { return const_arg; }

 private:
  bool check_args();

  distance_kind kind;
  double (*calc_distance_func)(float *v1, float *v2, size_t v_len);
  ha_rows m_limit = 0;
  Item_field *field_arg = nullptr;
  Item *const_arg = nullptr;
};

class Item_func_vec_distance_euclidean final : public Item_func_vec_distance {
 public:
  Item_func_vec_distance_euclidean(const POS &pos, Item *a, Item *b)
      : Item_func_vec_distance(pos, a, b, distance_kind::EUCLIDEAN) {}
};

class Item_func_vec_distance_cosine final : public Item_func_vec_distance {
 public:
  Item_func_vec_distance_cosine(const POS &pos, Item *a, Item *b)
      : Item_func_vec_distance(pos, a, b, distance_kind::COSINE) {}
};

class Item_func_vec_fromtext final : public Item_str_func {
  String buffer;

 public:
  Item_func_vec_fromtext(const POS &pos, Item *a) : Item_str_func(pos, a) {}
  bool resolve_type(THD *thd) override;
  const char *func_name() const override { return "VEC_FromText"; }
  String *val_str(String *str) override;
};

class Item_func_vec_totext final : public Item_str_func {
  static const uint32_t per_value_chars = 16;
  static const uint32_t max_output_bytes =
      (MAX_DIMENSIONS * Item_func_vec_totext::per_value_chars);
  String buffer;

 public:
  Item_func_vec_totext(const POS &pos, Item *a) : Item_str_func(pos, a) {
    collation.set(&my_charset_utf8mb4_0900_bin);
  }
  bool resolve_type(THD *thd) override;
  const char *func_name() const override { return "VEC_ToText"; }
  String *val_str(String *str) override;
};

class Item_func_vector_dim : public Item_int_func {
  String value;

 public:
  Item_func_vector_dim(const POS &pos, Item *a) : Item_int_func(pos, a) {}
  longlong val_int() override;
  const char *func_name() const override { return "vector_dim"; }
  bool resolve_type(THD *thd) override {
    if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_VARCHAR)) {
      return true;
    }
    bool valid_type = (args[0]->data_type() == MYSQL_TYPE_VARCHAR) ||
                      (args[0]->result_type() == STRING_RESULT &&
                       args[0]->collation.collation == &my_charset_bin);
    if (!valid_type) {
      my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
      return true;
    }
    max_length = 10;
    return false;
  }
};

static inline bool check_item_func_vec_distance(const Item *item) {
  return item->type() == Item::FUNC_ITEM &&
         ((Item_func *)item)->functype() == Item_func::VECTOR_DISTANCE_FUNC;
}
}  // namespace vidx

#endif /* VIDX_FUNC_INCLUDED */
