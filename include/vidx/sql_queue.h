/*
 Copyright (c) 2011, 2021, Oracle and/or its affiliates. All rights
 reserved.

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
 Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 02110-1301  USA
 */

#ifndef QUEUE_INCLUDED
#define QUEUE_INCLUDED

#include "sql/psi_memory_key.h"  // key_memory_vidx_mem
#include "storage/myisam/queues.h"

#define set_if_bigger(a, b)   \
  do {                        \
    if ((a) < (b)) (a) = (b); \
  } while (0)
#define set_if_smaller(a, b)  \
  do {                        \
    if ((a) > (b)) (a) = (b); \
  } while (0)

/* Attention, the QUEUE's elements in storage/myisam/queues.cc is started from
0 */
#define queue_first_element(queue) 0
#define queue_remove_top(queue_arg) \
  queue_remove((queue_arg), queue_first_element(queue_arg))

/**
  A typesafe wrapper of QUEUE, a priority heap
*/
template <typename Element, typename Param = void>
class Queue {
 public:
  Queue() { m_queue.root = 0; }
  ~Queue() { delete_queue(&m_queue); }
  int init(uint max_elements, bool max_at_top, queue_compare compare,
           Param *param = 0) {
    return init_queue(&m_queue, key_memory_vidx_mem, max_elements, 0,
                      max_at_top, compare, (void *)param);
  }

  size_t elements() const { return m_queue.elements; }
  bool is_inited() const { return is_queue_inited(&m_queue); }
  bool is_full() const { return queue_is_full((QUEUE *)(&m_queue)); }
  bool is_empty() const { return elements() == 0; }
  Element *top() const { return (Element *)queue_top(&m_queue); }

  void push(const Element *element) {
    queue_insert(&m_queue, (uchar *)element);
  }
  void safe_push(const Element *element) {
    if (is_full()) m_queue.elements--;  // remove one of the furthest elements
    queue_insert(&m_queue, (uchar *)element);
  }
  Element *pop() { return (Element *)queue_remove_top(&m_queue); }
  void clear() { queue_remove_all(&m_queue); }
  void propagate_top() { queue_replaced(&m_queue); }
  void replace_top(const Element *element) {
    queue_top(&m_queue) = (uchar *)element;
    propagate_top();
  }

 private:
  QUEUE m_queue;
};

#endif
