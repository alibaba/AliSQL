
#include "sql_filter.h"

/*
  lists bellow are used to store all filter items,
  according to the query type; Currently only support
  three type: SELECT, UPDATE, DELETE
*/
LIST *select_filter_list= NULL;
LIST *update_filter_list= NULL;
LIST *delete_filter_list= NULL;

/* if true, then the key words in filter item should
  be compared in order */
my_bool rds_key_cmp_in_order= false;

my_bool rds_reset_all_filter= false;

/* an increased value to incicate a filter item */
static int filter_item_id= 0;
#define SQL_FILTER_SPLIT '~'

/* check if the filter setting is right, currently only the format bellow is
  allowed:
  +,{CONC},INFO1~INFO2~INFO3...means add a new filter item, ONLY Conc
  threads that matchs INFO1/INFO2/INFO3...can be executed concurrently

  -,NUM1,NUM2,NUM3...means delete filter items with id NUM1, NUM2 and NUM3..
*/
my_bool check_sql_filter_valid(const char *str)
{
  if (!str || str[0] == '\0')
    return false;

  if (strlen(str) >= SQL_FILTER_STR_LEN)
    return true;

  /* the string must begin with +, or -, */
  if ((str[0] != '-' && str[0] != '+')
      || str[1] =='\0' || str[1] != ',')
    return true;

  const char *start, *end;
  end= str + 2;

  start= end;
  while (*end >= '0' && *end <= '9') // the second value should be a NUM
    end++;

  if (start == end) // such as "+,sd", is unvaild.
    return true;

  if (str[0] == '-' && *end == '\0') // such as "-,4" is valid
    return false;

  if (*end != ',')
    return true;

  uint comma_count= 0;
  end++;

  my_bool all_space= true;
  char split_char;
  if (str[0] == '-')
    split_char= ',';
  else
    split_char= SQL_FILTER_SPLIT;

  while (*end != '\0')
  {
    if (*end == split_char)
    {
      if (all_space) // such as '+,1,~' or '+,1,  ~'
        return true;

      all_space= true;
      comma_count++;
      end++;
      continue;
    }

    if (*end != ' ')
      all_space= false;

    if (str[0] == '-' && (*end < '0' || *end > '9'))
      return true;

    end++;
  }

  if (all_space || comma_count >= MAX_KEY_WORDS)
    return true;

  return false;
}

/* free a filter item */
void free_filter_item(filter_item *item)
{
  int i= 0;
  for (; i < item->key_num; ++i)
  {
    if (item->key_array[i])
      my_free(item->key_array[i]);
  }

  if (item->orig_str)
    my_free(item->orig_str);

  my_free(item);
}

/*
  Create a new filter according to item_str
  item_str should have a formated style, such as:
  "+,2,tb1~ a= 13"
    --> + means add a new item, 2 means only two threads
    that match "tb1" and "a= 13" were allowed to execute concurrently
*/
filter_item *create_filter_item(char *item_str)
{
  filter_item * item= (filter_item *)my_malloc
                      (sizeof(filter_item), MYF(MY_FAE | MY_ZEROFILL));

  DBUG_EXECUTE_IF("create_filter_item_error_0",
                  {
                    my_free(item);
                    item= NULL;
                  };);

  if (item == NULL)
    return NULL;

  item->id= __sync_add_and_fetch(&filter_item_id, 1); //start from 1
  item->max_conc= atoll(item_str + 2);
  item->cur_conc= 0;
  item->key_num= 0;

  char *begin, *end;

  char split_char[2];
  split_char[0]= SQL_FILTER_SPLIT;
  split_char[1]= '\0';

  begin= strstr(item_str + 3, ",");
  begin++;
  end= strstr(begin, split_char);

  /*  key words was seperated by '~` */
  while (end != NULL)
  {
    item->key_array[item->key_num++]=
                (char *) my_strndup(begin, end - begin, MYF(MY_FAE | MY_ZEROFILL));

    DBUG_EXECUTE_IF("create_filter_item_error_1",
                    {
                      my_free(item->key_array[item->key_num - 1]);
                      item->key_array[item->key_num - 1]= NULL;
                    };);

    if (item->key_array[item->key_num - 1] == NULL)
      goto error;

    begin= end + 1;
    end= strstr(begin, split_char);
  }

  item->key_array[item->key_num++]=
            (char *) my_strdup(begin, MYF(MY_FAE | MY_ZEROFILL));

  DBUG_EXECUTE_IF("create_filter_item_error_2",
                  {
                    my_free(item->key_array[item->key_num - 1]);
                    item->key_array[item->key_num - 1]= NULL;
                  };);

  if (item->key_array[item->key_num - 1] == NULL)
    goto error;

  /* store the item_str */
  item->orig_str= my_strdup(item_str, MYF(MY_FAE | MY_ZEROFILL));
  if (item->orig_str != NULL)
    goto end;

error:
  /* clean all allocted key because strdup filed */
  free_filter_item(item);
  item= NULL;
end:
  return item;
}

/*
  Called while adding new filter item according
  to global variable sql_select_filter/sql_delete_filter/sql_update_filter
*/
LIST *add_filter_item(LIST *filter_list, char *item_str)
{
  filter_item *item= create_filter_item(item_str);

  if (item == NULL)
    return NULL;

  LIST *node= (LIST *)my_malloc(sizeof(LIST), MYF(MY_FAE | MY_ZEROFILL));

  DBUG_EXECUTE_IF("add_filter_item_error_0",
                  {
                    my_free(node);
                    node= NULL;
                  };);
  /* failed to allocate memory */
  if (node == NULL)
  {
    free_filter_item(item);
    return NULL;
  }

  node->data= (void *)item;
  filter_list= list_add(filter_list, node);

  return filter_list;
}

LIST *find_node_by_id(LIST *filter_list, int item_id)
{
  filter_item *item= NULL;

  while (filter_list != NULL)
  {
    item= (filter_item *)filter_list->data;

    if (item->id == item_id)
      break;

    filter_list= filter_list->next;

  }

  return filter_list;
}

/*
  Free the filter item(stored int list->data) and
  delete the node from list
*/
LIST *free_item_by_id(LIST *filter_list, int id)
{
  LIST *node= find_node_by_id(filter_list, id);
  if (node)
  {
    filter_list= list_delete(filter_list, node);
    free_filter_item((filter_item *)node->data);
    my_free(node);
  }

  return filter_list;
}

/*
  This function was called while item_str is like:
  "-,3,5,6" means filter item with id 3,5,6 should be removed
*/
LIST *delete_filter_item(LIST *filter_list, const char *item_str)
{
  /* should't delete items from a NULL list */
  if (filter_list == NULL)
    return NULL;

  const char *begin;
  int item_id= 0;

  begin= item_str + 1;

  while (begin != NULL)
  {
    begin++;
    item_id= atoi(begin);

    filter_list= free_item_by_id(filter_list, item_id);

    begin= strstr(begin, ",");
  }

  return filter_list;
}

/*
  Reset a filter item list and free the memory
*/
void reset_filter_list(LIST *filter_list)
{
  LIST *next;
  while (filter_list != NULL)
  {
    next= filter_list->next;

    free_filter_item((filter_item *)filter_list->data);
    my_free(filter_list);

    filter_list= next;
  }
}

/*
  Check if there are any filter matches the SQL and update the cur_conc of the matched filter item
*/
my_bool find_matched_filter_and_update(LIST *filter_list, THD *thd)
{
  char *sql= thd->query();
  
  DBUG_ASSERT(sql);

  char *pos= NULL;
  filter_item *item= NULL;

  my_bool in_order= rds_key_cmp_in_order;

  while (filter_list != NULL)
  {
    int i=0;
    item= (filter_item*) filter_list->data;

    pos= sql;
    while (i < item->key_num)
    {
      pos= strstr(pos, item->key_array[i]);
      if (!pos)
        break;

      if (!in_order)
        pos= sql;
      else
        pos= pos + strlen(item->key_array[i]);

      i++;
    }

    if (i == item->key_num) //all key matches
      break;

    filter_list= filter_list->next;
  } 

  if (filter_list == NULL) //no matched item
    return false;

  /*
    if the max allowed conc is 0, then we don't need to check cur_conc,
    just return immediately
  */
  if (item->max_conc == 0)
    return true;
  /* here we find a filter item that matchs the sql */
  long long store_val= 0;
retry:
  store_val= __sync_add_and_fetch(&(item->cur_conc), 0);
  if (store_val == item->max_conc)
    return true;

  DEBUG_SYNC_C("find_matched_filter_and_update_after_get_cur_conc");

  if (!__sync_bool_compare_and_swap(&(item->cur_conc),
                                    store_val, store_val + 1))
    goto retry;

  thd->filter_id= item->id;

  return false;
}

/*
  Check if current statement need traffic control.
*/
my_bool need_traffic_control(THD *thd, uint command)
{
  int ret= false;

  switch(command)
  {
    case SQLCOM_SELECT:
      if (!select_filter_list) //check without lock
        break;

      mysql_rwlock_rdlock(&LOCK_filter_list);
      DBUG_EXECUTE_IF("check_lock_filter_list_sync",
          {
            sleep(2);
          };);
      ret= find_matched_filter_and_update(select_filter_list, thd);
      mysql_rwlock_unlock(&LOCK_filter_list);
      break;

      case SQLCOM_UPDATE:
      case SQLCOM_UPDATE_MULTI:
        if (!update_filter_list)
          break;

        mysql_rwlock_rdlock(&LOCK_filter_list);
        ret= find_matched_filter_and_update(update_filter_list, thd);
        mysql_rwlock_unlock(&LOCK_filter_list);
        break;

      case SQLCOM_DELETE:
      case SQLCOM_DELETE_MULTI:
        if (!delete_filter_list)
          break;

        mysql_rwlock_rdlock(&LOCK_filter_list);
        ret= find_matched_filter_and_update(delete_filter_list, thd);
        mysql_rwlock_unlock(&LOCK_filter_list);
        break;

      default:
        break;
  }

  return ret;
}

/*
  decrease the counter of item->cur_conc
*/
void dec_filter_item_conc(THD *thd, uint command)
{
  /*
    filter_id is 0 if the current SQL is not filted or the max allowed
    concurency of the matched filter item is zer0
  */
  if (thd->filter_id == 0)
    return;

  LIST *node= NULL;

  mysql_rwlock_rdlock(&LOCK_filter_list);
  switch (command)
  {
    case SQLCOM_SELECT:
      node= find_node_by_id(select_filter_list, thd->filter_id);
      break;
    case SQLCOM_UPDATE:
    case SQLCOM_UPDATE_MULTI:
      node= find_node_by_id(update_filter_list, thd->filter_id);
      break;
    case SQLCOM_DELETE:
    case SQLCOM_DELETE_MULTI:
      node= find_node_by_id(delete_filter_list, thd->filter_id);
      break;
    default:
      break;
  }
  
  if (node)
  {
    filter_item *item= (filter_item *)node->data;
    __sync_sub_and_fetch(&(item->cur_conc), 1);
  }

  mysql_rwlock_unlock(&LOCK_filter_list);
  thd->filter_id= 0;

  return;
}
