
#ifndef SQL_FILTER_INCLUDED
#define SQL_FILTER_INCLUDED

#include "mysqld.h"
#include "sql_class.h"
#include "my_list.h"
#include "sql_const.h" 

#define MAX_KEY_WORDS 128

typedef struct filter_item
{
  int id; /* an increased value to indicate this item */
  long long max_conc; /* max statements can be executed concurrently */
  long long cur_conc; /* currenty conc statements */
  int key_num; /* key num of this filter */
  char * key_array[MAX_KEY_WORDS]; /* pointers to store key string */
  char *orig_str; /* used to print in I_S table */
} filter_item;

extern LIST *select_filter_list;
extern LIST *update_filter_list;
extern LIST *delete_filter_list;
extern my_bool rds_key_cmp_in_order;
extern my_bool rds_reset_all_filter;

my_bool need_traffic_control(THD *thd, uint command);
my_bool check_sql_filter_valid(const char *str);
LIST *add_filter_item(LIST *filter_list, char *item_str);
LIST *delete_filter_item(LIST *filter_list, const char *item_str);
void reset_filter_list(LIST *filter_list);
void dec_filter_item_conc(THD *thd, uint command);

#endif /* SQL_FILTER_INCLUDED */

