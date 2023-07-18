-- This file should undo anything in `up.sql`
DROP TABLE IF EXISTS objects;
DROP INDEX IF EXISTS o_owner_idx;
DROP INDEX IF EXISTS o_object_skh_idx;
DROP INDEX IF EXISTS o_skh_idx;
DROP INDEX IF EXISTS o_insat_idx;
DROP TABLE IF EXISTS current_objects;
DROP INDEX IF EXISTS co_owner_idx;
DROP INDEX IF EXISTS co_object_skh_idx;
DROP INDEX IF EXISTS co_skh_idx;
DROP INDEX IF EXISTS co_insat_idx;
ALTER TABLE move_resources DROP COLUMN IF EXISTS state_key_hash;
DROP TABLE IF EXISTS token_ownerships_v2;
DROP INDEX IF EXISTS to2_id_index;
DROP INDEX IF EXISTS to2_owner_index;
DROP INDEX IF EXISTS to2_insat_index;
DROP TABLE IF EXISTS current_token_ownerships_v2;
DROP INDEX IF EXISTS curr_to2_owner_index;
DROP INDEX IF EXISTS curr_to2_wa_index;
DROP INDEX IF EXISTS curr_to2_insat_index;
DROP TABLE IF EXISTS collections_v2;
DROP INDEX IF EXISTS col2_id_index;
DROP INDEX IF EXISTS col2_crea_cn_index;
DROP INDEX IF EXISTS col2_insat_index;
DROP TABLE IF EXISTS current_collections_v2;
DROP INDEX IF EXISTS cur_col2_crea_cn_index;
DROP INDEX IF EXISTS cur_col2_insat_index;
DROP TABLE IF EXISTS token_datas_v2;
DROP INDEX IF EXISTS td2_id_index;
DROP INDEX IF EXISTS td2_cid_name_index;
DROP INDEX IF EXISTS td2_insat_index;
DROP TABLE IF EXISTS current_token_datas_v2;
DROP INDEX IF EXISTS cur_td2_cid_name_index;
DROP INDEX IF EXISTS cur_td2_insat_index;
ALTER TABLE current_token_pending_claims DROP COLUMN IF EXISTS token_data_id;
ALTER TABLE current_token_pending_claims DROP COLUMN IF EXISTS collection_id;