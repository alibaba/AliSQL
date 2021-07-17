/* Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/* Routines to handle mallocing of results which will be freed the same time */

#include <my_global.h>
#include <my_sys.h>
#include <m_string.h>
#undef EXTRA_DEBUG
#define EXTRA_DEBUG

static THD_MALLOC_SIZE_CB query_size_cb_func= NULL;

void set_thd_query_size_cb(THD_MALLOC_SIZE_CB func)
{
  query_size_cb_func= func;
}

static void update_query_size(MEM_ROOT *root, size_t length, int flag)
{
  if (query_size_cb_func)
    query_size_cb_func(length, flag, root);
}

/*
  Initialize memory root

  SYNOPSIS
    init_alloc_root()
      mem_root       - memory root to initialize
      block_size     - size of chunks (blocks) used for memory allocation
                       (It is external size of chunk i.e. it should include
                        memory required for internal structures, thus it
                        should be no less than ALLOC_ROOT_MIN_BLOCK_SIZE)
      pre_alloc_size - if non-0, then size of block that should be
                       pre-allocated during memory root initialization.

  DESCRIPTION
    This function prepares memory root for further use, sets initial size of
    chunk for memory allocation and pre-allocates first block if specified.
    Altough error can happen during execution of this function if
    pre_alloc_size is non-0 it won't be reported. Instead it will be
    reported as error in first alloc_root() on this memory root.
*/

/**
 *
   wangyang 这里的目的是初始化 mem_root 对象 ， 前面传递过来的是一个空结构体，
   下面会对内存部分 进行相应的初始化

 */
void init_alloc_root(MEM_ROOT *mem_root, size_t block_size,
		     size_t pre_alloc_size MY_ATTRIBUTE((unused)))
{
  DBUG_ENTER("init_alloc_root");
  DBUG_PRINT("enter",("root: 0x%lx", (long) mem_root));

  mem_root->free= mem_root->used= mem_root->pre_alloc= 0;
  mem_root->min_malloc= 32;
  mem_root->block_size= block_size - ALLOC_ROOT_MIN_BLOCK_SIZE;
  mem_root->error_handler= 0;
  mem_root->block_num= 4;			/* We shift this with >>2 */
  mem_root->first_block_usage= 0;

#if !(defined(HAVE_purify) && defined(EXTRA_DEBUG))
  /**
   * wangyang 下面这一块 表示一定的预分配
   */
  if (pre_alloc_size)
  {
    if ((mem_root->free= mem_root->pre_alloc=
	 (USED_MEM*) my_malloc(pre_alloc_size+ ALIGN_SIZE(sizeof(USED_MEM)),
			       MYF(0))))
    {
      mem_root->free->size= pre_alloc_size+ALIGN_SIZE(sizeof(USED_MEM));
      mem_root->free->left= pre_alloc_size;
      mem_root->free->next= 0;
      update_query_size(mem_root, mem_root->free->size, 0);
    }
  }
#endif
  DBUG_VOID_RETURN;
}

/** This is a no-op unless the build is debug or for Valgrind. */
#define TRASH_MEM(X) TRASH(((char*)(X) + ((X)->size-(X)->left)), (X)->left)


/*
  SYNOPSIS
    reset_root_defaults()
    mem_root        memory root to change defaults of
    block_size      new value of block size. Must be greater or equal
                    than ALLOC_ROOT_MIN_BLOCK_SIZE (this value is about
                    68 bytes and depends on platform and compilation flags)
    pre_alloc_size  new size of preallocated block. If not zero,
                    must be equal to or greater than block size,
                    otherwise means 'no prealloc'.
  DESCRIPTION
    Function aligns and assigns new value to block size; then it tries to
    reuse one of existing blocks as prealloc block, or malloc new one of
    requested size. If no blocks can be reused, all unused blocks are freed
    before allocation.
*/

void reset_root_defaults(MEM_ROOT *mem_root, size_t block_size,
                         size_t pre_alloc_size MY_ATTRIBUTE((unused)))
{
  DBUG_ASSERT(alloc_root_inited(mem_root));

  mem_root->block_size= block_size - ALLOC_ROOT_MIN_BLOCK_SIZE;
#if !(defined(HAVE_purify) && defined(EXTRA_DEBUG))
  if (pre_alloc_size)
  {
    size_t size= pre_alloc_size + ALIGN_SIZE(sizeof(USED_MEM));
    if (!mem_root->pre_alloc || mem_root->pre_alloc->size != size)
    {
      USED_MEM *mem, **prev= &mem_root->free;
      /*
        Free unused blocks, so that consequent calls
        to reset_root_defaults won't eat away memory.
      */
      while (*prev)
      {
        mem= *prev;
        if (mem->size == size)
        {
          /* We found a suitable block, no need to do anything else */
          mem_root->pre_alloc= mem;
          return;
        }
        if (mem->left + ALIGN_SIZE(sizeof(USED_MEM)) == mem->size)
        {
          /* remove block from the list and free it */
          *prev= mem->next;
          {
            mem->left= mem->size;
            TRASH_MEM(mem);
            my_free(mem);
          }
        }
        else
          prev= &mem->next;
      }
      /* Allocate new prealloc block and add it to the end of free list */
      if ((mem= (USED_MEM *) my_malloc(size, MYF(0))))
      {
        mem->size= size; 
        mem->left= pre_alloc_size;
        mem->next= *prev;
        *prev= mem_root->pre_alloc= mem; 
      }
      else
      {
        mem_root->pre_alloc= 0;
      }
    }
  }
  else
#endif
    mem_root->pre_alloc= 0;
}


void *alloc_root(MEM_ROOT *mem_root, size_t length)
{
#if defined(HAVE_purify) && defined(EXTRA_DEBUG)
  reg1 USED_MEM *next;
  DBUG_ENTER("alloc_root");
  DBUG_PRINT("enter",("root: 0x%lx", (long) mem_root));

  DBUG_ASSERT(alloc_root_inited(mem_root));

  DBUG_EXECUTE_IF("simulate_out_of_memory",
                  {
                    if (mem_root->error_handler)
                      (*mem_root->error_handler)();
                    DBUG_SET("-d,simulate_out_of_memory");
                    DBUG_RETURN((void*) 0); /* purecov: inspected */
                  });

  length+=ALIGN_SIZE(sizeof(USED_MEM));
  if (!(next = (USED_MEM*) my_malloc(length,MYF(MY_WME | ME_FATALERROR))))
  {
    if (mem_root->error_handler)
      (*mem_root->error_handler)();
    DBUG_RETURN((uchar*) 0);			/* purecov: inspected */
  }
  update_query_size(mem_root, length, 1);
  next->next= mem_root->used;
  next->size= length;
  mem_root->used= next;
  DBUG_PRINT("exit",("ptr: 0x%lx", (long) (((char*) next)+
                                           ALIGN_SIZE(sizeof(USED_MEM)))));
  DBUG_RETURN((uchar*) (((char*) next)+ALIGN_SIZE(sizeof(USED_MEM))));
#else
  size_t get_size, block_size;
  uchar* point;
  reg1 USED_MEM *next= 0;
  reg2 USED_MEM **prev;
  DBUG_ENTER("alloc_root");
  DBUG_PRINT("enter",("root: 0x%lx", (long) mem_root));
  DBUG_ASSERT(alloc_root_inited(mem_root));

  DBUG_EXECUTE_IF("simulate_out_of_memory",
                  {
                    /* Avoid reusing an already allocated block */
                    if (mem_root->error_handler)
                      (*mem_root->error_handler)();
                    DBUG_SET("-d,simulate_out_of_memory");
                    DBUG_RETURN((void*) 0); /* purecov: inspected */
                  });
  length= ALIGN_SIZE(length);
  if ((*(prev= &mem_root->free)) != NULL)
  {
    if ((*prev)->left < length &&
	mem_root->first_block_usage++ >= ALLOC_MAX_BLOCK_USAGE_BEFORE_DROP &&
	(*prev)->left < ALLOC_MAX_BLOCK_TO_DROP)
    {
        /**
         * wangyang 如果 剩余 已经少于length 那么移除 free list 中
         * 移入到 used 列表当中
         */
      next= *prev;
      *prev= next->next;			/* Remove block from list */
      next->next= mem_root->used;
      mem_root->used= next;
      mem_root->first_block_usage= 0;
    }
    /**
     * wangyang 这里会按照 free 指针进行寻找，直到找到 left > length 的内存块
     */
    for (next= *prev ; next && next->left < length ; next= next->next)
      prev= &next->next;
  }
  if (! next)
  {						/* Time to alloc new block */
      // block_nun = 4 >> 2 ==> 1
    block_size= mem_root->block_size * (mem_root->block_num >> 2);
    get_size= length+ALIGN_SIZE(sizeof(USED_MEM));
    get_size= MY_MAX(get_size, block_size); // wangyang @@ 这里会 比较 获取 最大的值

    if (!(next = (USED_MEM*) my_malloc(get_size,MYF(MY_WME | ME_FATALERROR))))
    {
      if (mem_root->error_handler)
	(*mem_root->error_handler)();
      DBUG_RETURN((void*) 0);                      /* purecov: inspected */
    }
    /**
     * wangyang prev 指针 表示的是 free 指针
     * 这里主要是将 新增加的 内存块 放置在 最前面
     */
    update_query_size(mem_root, length, 1);
    mem_root->block_num++; // 分配的块的数量 增加
    next->next= *prev;
    next->size= get_size; //
    next->left= get_size-ALIGN_SIZE(sizeof(USED_MEM));
    *prev=next;
  }

  /**
   * wangyang 这里会 确保
   */
  point= (uchar*) ((char*) next+ (next->size-next->left));
  /*TODO: next part may be unneded due to mem_root->first_block_usage counter*/
  /**
   * wangyang 这里会判断 ，如果 剩余 内存大小, 小于 最小分配大小，那么放入 used 链表当中
   */
  if ((next->left-= length) < mem_root->min_malloc)
  {						/* Full block */
    *prev= next->next;				/* Remove block from list */
    next->next= mem_root->used;
    mem_root->used= next;
    mem_root->first_block_usage= 0;
  }
  DBUG_PRINT("exit",("ptr: 0x%lx", (ulong) point));
  DBUG_RETURN((void*) point);
#endif
}


/*
  Allocate many pointers at the same time.

  DESCRIPTION
    ptr1, ptr2, etc all point into big allocated memory area.

  SYNOPSIS
    multi_alloc_root()
      root               Memory root
      ptr1, length1      Multiple arguments terminated by a NULL pointer
      ptr2, length2      ...
      ...
      NULL

  RETURN VALUE
    A pointer to the beginning of the allocated memory block
    in case of success or NULL if out of memory.
*/

void *multi_alloc_root(MEM_ROOT *root, ...)
{
  va_list args;
  char **ptr, *start, *res;
  size_t tot_length, length;
  DBUG_ENTER("multi_alloc_root");

  /**
   * va_start 在使用的时候 第一个参数是 va_list , 用于承载所有的 变长参数
   * 第二个参数是 变长参数前最后一个 参数，用于确认分界
   */
  va_start(args, root);
  tot_length= 0;
  /**
   * wangyang 使用 va_arg 的作用是 用于从 va_list 中获取 某个参数
   *
   */
  while ((ptr= va_arg(args, char **))) //这里的目的是从va_list中获取一个参数，类型是 char** 类型
  {
    length= va_arg(args, uint); // 这里是从va_list中获取参数，类型是uint 目的是用于确认相应的大小(内存大小)
    tot_length+= ALIGN_SIZE(length);// 这里对总的内存大小进行累加
  }
  va_end(args);

  if (!(start= (char*) alloc_root(root, tot_length)))
    DBUG_RETURN(0);                            /* purecov: inspected */

  va_start(args, root);
  res= start;
  /*
   * 这里的目的是用来 给相应的指针分配地址 ，ptr是二级指针
   * 然后使用 *ptr 来确认 分配的 地址
   */
  while ((ptr= va_arg(args, char **)))
  {
    *ptr= res;
    length= va_arg(args, uint);
    res+= ALIGN_SIZE(length); // 这里用于对齐内存
  }
  va_end(args);
  DBUG_RETURN((void*) start);
}

/* Mark all data in blocks free for reusage */

static inline void mark_blocks_free(MEM_ROOT* root)
{
  reg1 USED_MEM *next;
  reg2 USED_MEM **last;

  /* iterate through (partially) free blocks, mark them free */
  last= &root->free;
  for (next= root->free; next; next= *(last= &next->next))
  {
    next->left= next->size - ALIGN_SIZE(sizeof(USED_MEM));
    TRASH_MEM(next);
  }

  /* Combine the free and the used list */
  *last= next=root->used;

  /* now go through the used blocks and mark them free */
  for (; next; next= next->next)
  {
    next->left= next->size - ALIGN_SIZE(sizeof(USED_MEM));
    TRASH_MEM(next);
  }

  /* Now everything is set; Indicate that nothing is used anymore */
  root->used= 0;
  root->first_block_usage= 0;
}


/*
  Deallocate everything used by alloc_root or just move
  used blocks to free list if called with MY_USED_TO_FREE

  SYNOPSIS
    free_root()
      root		Memory root
      MyFlags		Flags for what should be freed:

        MY_MARK_BLOCKS_FREED	Don't free blocks, just mark them free
        MY_KEEP_PREALLOC	If this is not set, then free also the
        		        preallocated block

  NOTES
    One can call this function either with root block initialised with
    init_alloc_root() or with a zero()-ed block.
    It's also safe to call this multiple times with the same mem_root.
*/

void free_root(MEM_ROOT *root, myf MyFlags)
{
  reg1 USED_MEM *next,*old;
  ulonglong mem_size= 0;
  DBUG_ENTER("free_root");
  DBUG_PRINT("enter",("root: 0x%lx  flags: %u", (long) root, (uint) MyFlags));

  if (MyFlags & MY_MARK_BLOCKS_FREE)
  {
    mark_blocks_free(root);
    DBUG_VOID_RETURN;
  }
  if (!(MyFlags & MY_KEEP_PREALLOC))
    root->pre_alloc=0;

  for (next=root->used; next ;)
  {
    old=next; next= next->next ;
    if (old != root->pre_alloc)
    {
      old->left= old->size;
      TRASH_MEM(old);
      my_free(old);
    }
  }
  for (next=root->free ; next ;)
  {
    old=next; next= next->next;
    if (old != root->pre_alloc)
    {
      old->left= old->size;
      TRASH_MEM(old);
      my_free(old);
    }
  }
  root->used=root->free=0;
  if (root->pre_alloc)
  {
    root->free=root->pre_alloc;
    root->free->left=root->pre_alloc->size-ALIGN_SIZE(sizeof(USED_MEM));
    TRASH_MEM(root->pre_alloc);
    root->free->next=0;
    mem_size= root->pre_alloc->size;
  }
  update_query_size(root, mem_size, 0);
  root->block_num= 4;
  root->first_block_usage= 0;
  DBUG_VOID_RETURN;
}

/*
  Find block that contains an object and set the pre_alloc to it
*/

void set_prealloc_root(MEM_ROOT *root, char *ptr)
{
  USED_MEM *next;
  for (next=root->used; next ; next=next->next)
  {
    if ((char*) next <= ptr && (char*) next + next->size > ptr)
    {
      root->pre_alloc=next;
      return;
    }
  }
  for (next=root->free ; next ; next=next->next)
  {
    if ((char*) next <= ptr && (char*) next + next->size > ptr)
    {
      root->pre_alloc=next;
      return;
    }
  }
}


char *strdup_root(MEM_ROOT *root, const char *str)
{
  return strmake_root(root, str, strlen(str));
}


char *strmake_root(MEM_ROOT *root, const char *str, size_t len)
{
  char *pos;
  if ((pos=alloc_root(root,len+1)))
  {
    memcpy(pos,str,len);
    pos[len]=0;
  }
  return pos;
}


void *memdup_root(MEM_ROOT *root, const void *str, size_t len)
{
  char *pos;
  if ((pos=alloc_root(root,len)))
    memcpy(pos,str,len);
  return pos;
}
