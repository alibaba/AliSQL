#
# get_ndb_connection_id.inc - get API node id used by this session
#
# Usage
# --source suite/ndb/include/get_ndb_connection_id.inc
# 
# $ndb_connection_id will be set to node id of current 
# session's API connection

--disable_query_log
--disable_result_log
create table get_ndb_connection_id(a int primary key) engine=ndb;
begin;
insert into get_ndb_connection_id values (1);

let $ndb_connection_id = query_get_value(select client_node_id from ndbinfo.cluster_operations where tableid = (select id from ndbinfo.dict_obj_info where fq_name like '%get_ndb_connection_id%') limit 1, client_node_id, 1);
rollback;
drop table get_ndb_connection_id;
--enable_result_log
--enable_query_log
