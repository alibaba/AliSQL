/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:

/*
COPYING CONDITIONS NOTICE:

  This program is free software; you can redistribute it and/or modify
  it under the terms of version 2 of the GNU General Public License as
  published by the Free Software Foundation, and provided that the
  following conditions are met:

      * Redistributions of source code must retain this COPYING
        CONDITIONS NOTICE, the COPYRIGHT NOTICE (below), the
        DISCLAIMER (below), the UNIVERSITY PATENT NOTICE (below), the
        PATENT MARKING NOTICE (below), and the PATENT RIGHTS
        GRANT (below).

      * Redistributions in binary form must reproduce this COPYING
        CONDITIONS NOTICE, the COPYRIGHT NOTICE (below), the
        DISCLAIMER (below), the UNIVERSITY PATENT NOTICE (below), the
        PATENT MARKING NOTICE (below), and the PATENT RIGHTS
        GRANT (below) in the documentation and/or other materials
        provided with the distribution.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
  02110-1301, USA.

COPYRIGHT NOTICE:

  TokuFT, Tokutek Fractal Tree Indexing Library.
  Copyright (C) 2007-2013 Tokutek, Inc.

DISCLAIMER:

  This program is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

UNIVERSITY PATENT NOTICE:

  The technology is licensed by the Massachusetts Institute of
  Technology, Rutgers State University of New Jersey, and the Research
  Foundation of State University of New York at Stony Brook under
  United States of America Serial No. 11/760379 and to the patents
  and/or patent applications resulting from it.

PATENT MARKING NOTICE:

  This software is covered by US Patent No. 8,185,551.
  This software is covered by US Patent No. 8,489,638.

PATENT RIGHTS GRANT:

  "THIS IMPLEMENTATION" means the copyrightable works distributed by
  Tokutek as part of the Fractal Tree project.

  "PATENT CLAIMS" means the claims of patents that are owned or
  licensable by Tokutek, both currently or in the future; and that in
  the absence of this license would be infringed by THIS
  IMPLEMENTATION or by using or running THIS IMPLEMENTATION.

  "PATENT CHALLENGE" shall mean a challenge to the validity,
  patentability, enforceability and/or non-infringement of any of the
  PATENT CLAIMS or otherwise opposing any of the PATENT CLAIMS.

  Tokutek hereby grants to you, for the term and geographical scope of
  the PATENT CLAIMS, a non-exclusive, no-charge, royalty-free,
  irrevocable (except as stated in this section) patent license to
  make, have made, use, offer to sell, sell, import, transfer, and
  otherwise run, modify, and propagate the contents of THIS
  IMPLEMENTATION, where such license applies only to the PATENT
  CLAIMS.  This grant does not include claims that would be infringed
  only as a consequence of further modifications of THIS
  IMPLEMENTATION.  If you or your agent or licensee institute or order
  or agree to the institution of patent litigation against any entity
  (including a cross-claim or counterclaim in a lawsuit) alleging that
  THIS IMPLEMENTATION constitutes direct or contributory patent
  infringement, or inducement of patent infringement, then any rights
  granted to you under this License shall terminate as of the date
  such litigation is filed.  If you or your agent or exclusive
  licensee institute or order or agree to the institution of a PATENT
  CHALLENGE, then Tokutek may terminate any rights granted to you
  under this License.
*/

/**
 * Test that read committed always isolation works.
 *
 * Read committed means 'always read the outermost committed value'. This is less isolated
 * than 'read committed', which MySQl defines as 'snapshot isolation per sub-statement (child txn)'
 */

#include <portability/toku_random.h>

#include "test.h"

static void test_simple_committed_read(DB_ENV *env) {
    int r;
    DB *db;
    r = db_create(&db, env, 0); CKERR(r);
    r = db->open(db, NULL, "db", NULL, DB_BTREE, DB_CREATE, 0644); CKERR(r);

    char valbuf[64];
    DBT john, christian, val;
    dbt_init(&john, "john", sizeof("john"));
    dbt_init(&christian, "christian", sizeof("christian"));
    dbt_init(&val, valbuf, sizeof(valbuf));

    // start with just john
    r = db->put(db, NULL, &john, &john, 0); CKERR(r);

    // begin an outer txn with read-committed-always isolation
    DB_TXN *outer_txn;
    r = env->txn_begin(env, NULL, &outer_txn, DB_READ_COMMITTED_ALWAYS); CKERR(r);

    // outer txn sees john
    r = db->get(db, outer_txn, &john, &val, 0); CKERR(r);

    // outer txn does not yet see christian
    r = db->get(db, outer_txn, &christian, &val, 0); CKERR2(r, DB_NOTFOUND);

    // insert christian in another txn (NULL means generate an auto-commit txn)
    r = db->put(db, NULL, &christian, &christian, 0); CKERR(r);

    // outer txn does not see christian, because it is provisional
    // and our copied snapshot says it is not committed
    r = db->get(db, outer_txn, &christian, &val, 0); CKERR2(r, DB_NOTFOUND);

    // insert christian in another txn (again), thereby autocommitting last put
    r = db->put(db, NULL, &christian, &christian, 0); CKERR(r);

    // outer txn sees christian, because we now have a committed version
    r = db->get(db, outer_txn, &christian, &val, 0); CKERR(r);

    // delete john in another txn
    r = db->del(db, NULL, &john, 0); CKERR(r);

    // outer txn no longer sees john
    r = db->get(db, outer_txn, &john, &val, 0); CKERR2(r, DB_NOTFOUND);

    r = outer_txn->commit(outer_txn, 0); CKERR(r);

    r = db->close(db, 0); CKERR(r);
    r = env->dbremove(env, NULL, "db", NULL, 0); CKERR(r);
}

int test_main(int argc, char * const argv[]) {
    default_parse_args(argc, argv);

    int r;
    const int envflags = DB_INIT_MPOOL | DB_CREATE | DB_THREAD |
                         DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_TXN | DB_PRIVATE;

    // startup
    DB_ENV *env;
    toku_os_recursive_delete(TOKU_TEST_FILENAME);
    r = toku_os_mkdir(TOKU_TEST_FILENAME, 0755); CKERR(r);
    r = db_env_create(&env, 0); CKERR(r);
    r = env->open(env, TOKU_TEST_FILENAME, envflags, 0755);

    test_simple_committed_read(env);

    // cleanup
    r = env->close(env, 0); CKERR(r);

    return 0;
}

