from fastapi import HTTPException

class HelperFunctions():
    def __init__(self, db_instance):
        self._db_instance = db_instance
    
    def get_calc_views(self, source_schema_name, table_name_like, cdc_type):

        if cdc_type is None:
            sql_command =   "SELECT TABLE_NAME, CALC_VIEW_SOURCE " \
                            "FROM   \"ENVIRONMENTADMIN\".\"bi.ddl.ENVIRONMENTADMIN.functions.gen_dp_views::TF_GEN_SCHEMA_DP_CALC_VIEWS\"  " \
                            "  ( " \
                            "	P_SOURCE_SCHEMA_NAME => '" + source_schema_name + "', " \
                            "	P_TABLE_NAMES_LIKE   => '" + table_name_like + "', " \
                            "	P_TRAILING_DELIM     => '' " \
                            "  )"
        else:
            sql_command =   "SELECT TABLE_NAME, CALC_VIEW_SOURCE " \
                            "FROM   \"ENVIRONMENTADMIN\".\"bi.ddl.ENVIRONMENTADMIN.functions.gen_dp_views::TF_GEN_SCHEMA_DP_CALC_VIEWS_CDC\"  " \
                            "  ( " \
                            "	P_SOURCE_SCHEMA_NAME => '" + source_schema_name + "', " \
                            "	P_TARGET_TYPE        => '" + cdc_type.upper() + "', " \
                            "	P_TABLE_NAMES_LIKE   => '" + table_name_like + "', " \
                            "	P_TRAILING_DELIM     => '' " \
                            "  )"

        
        try:
            rows = self._db_instance.exec_query(sql_command)
            return rows

        except Exception as e:
            raise e

    def get_role_defs(self, workload, environment, role_def_name_like):
        sql_command =   "CALL \"ENVIRONMENTADMIN\".\"bi.ddl.ENVIRONMENTADMIN.functions.gen_roles::prc_generate_role_def\"  " \
                        "  ( " \
                        "	p_workload           => '" + workload + "', " \
                        "	p_environment        => '" + environment  + "', " \
                        "	p_role_def_name_like => '" + role_def_name_like + "' " \
                        "  )"

        try:
            rows = self._db_instance.exec_query(sql_command)
            return rows

        except Exception as e:
            raise e
        
        finally:
            if rows is None:
                raise HTTPException(400, "invalid request body")

    def get_change_lists(self, package_id_like):
        sql_command =   "SELECT 'BID@BID//' || c.change_number AS CHANGE_NUMBER, ce.package_id, ce.object_name, ce.object_suffix " \
                        "FROM   _sys_repo.CHANGES c, _sys_repo.CHANGE_ENTRIES ce " \
                        "WHERE  c.change_number = ce.change_number " \
                        "AND    c.src_system = ce.src_system " \
                        "AND    c.src_system = 'BID@BID'  " \
                        "AND    ce.package_id like ' " + package_id_like + "' " \
                        "and    c.released_at IS NULL " \
                        "ORDER BY created_at desc;"

        try:
            rows = self._db_instance.exec_query(sql_command)
            return rows

        except Exception as e:
            self._logger_instance.error('Exception getting change lists!!', exc_info=True)
            return None

    def get_cdc_count(self, schema_name):
        sql_command =   "SELECT COUNT(*) AS TABLE_CNT, " \
                        "       SUM(CASE WHEN TABLE_NAME LIKE 'V$_%$_CURRENT' ESCAPE '$' THEN 1 ELSE 0 END) AS CDC_CNT " \
                        "FROM   TABLES " \
                        "WHERE  SCHEMA_NAME = '" + schema_name + "' "

        try:
            rows = self._db_instance.exec_query(sql_command)
            for row in rows:
                table_cnt = row['TABLE_CNT']
                cdc_cnt = row['CDC_CNT']

            return table_cnt, cdc_cnt

        except Exception as e:
            raise e

    def get_schema_count(self, schema_name):
        sql_command =   "SELECT COUNT(*) " \
                        "FROM   SCHEMAS  " \
                        "WHERE  schema_name = ' " + schema_name + "' "

        try:
            rows = self._db_instance.exec_query(sql_command)

            return rows

        except Exception as e:
            self._logger_instance.error('Exception getting schema count!!', exc_info=True)
            return None

    def get_virtual_table_names(
            self, 
            source_database, 
            target_schema, 
            table_name_like='%', 
            table_name_not_like='', 
            ignore_raw_tables=True, 
            ignore_history_tables=True,
            ignore_standard_tables=True):
        
        sql_command =   "SELECT * FROM \"ENVIRONMENTADMIN\".\"bi.ddl.ENVIRONMENTADMIN.functions::FN_GET_VIRTUAL_TABLE_NAMES\"  " \
                        "  ( " \
                        "   p_source_database        => '" + source_database + "', " \
                        "   p_target_schema          => '" + target_schema  + "', " \
                        "   p_ignore_raw_tables      => "  + str(ignore_raw_tables) + ", " \
                        "   p_ignore_history_tables  => "  + str(ignore_history_tables) + ", " \
                        "   p_ignore_standard_tables => "  + str(ignore_standard_tables) + ", " \
                        "   P_table_names_like       => '" + table_name_like + "', " \
                        "   P_table_names_not_like   => '" + table_name_not_like + "' " \
                        "  ) "

        try:
            rows = self._db_instance.exec_query(sql_command)

        except Exception as e:
            rows = None
            raise e
        
        finally:
            if rows is None:
                raise HTTPException(400, "invalid request body")
            return rows
        
    def try_provision_virtual_table(self, source_table, target_table):
        sql_command =   "CALL \"ENVIRONMENTADMIN\".\"bi.ddl.ENVIRONMENTADMIN.procedures::try_provision_table\"  " \
                        "  ( " \
                        "	source => '" + source_table + "', " \
                        "	target => '" + target_table + "' " \
                        "  )"

        try:
            rows = self._db_instance.execute(sql_command)

            # Virtualisation succeeded, return True
            return True

        except Exception as e:
            # Virtualisation failed, return False
            return False
        
    def try_refresh_virtual_table(self, target_table):
        sql_command =   "CALL \"ENVIRONMENTADMIN\".\"bi.ddl.ENVIRONMENTADMIN.procedures::try_refresh_vtable\"  " \
                        "  ( " \
                        "	target => '" + target_table + "' " \
                        "  )"

        try:
            rows = self._db_instance.execute(sql_command)
            # Virtualisation succeeded, return True
            return True

        except Exception as e:
            # Virtualisation failed, return False
            return False



    def get_unrestricted_role_def(self, role_type):
        sql_command =   "SELECT * FROM \"ENVIRONMENTADMIN\".\"bi.ddl.ENVIRONMENTADMIN.functions::FN_CREATE_DYNAMIC_ROLE_IMPL\"  " \
                        "  ( " \
                        "	RoleTypeToCreate => " + str(role_type) + " " \
                        "  )"

        try:
            rows = self._db_instance.exec_query(sql_command)
            return rows

        except Exception as e:
            raise e
        
        finally:
            if rows is None:
                raise HTTPException(400, "invalid request body")