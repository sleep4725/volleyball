import pymysql
import yaml
import os
##
#
## ==================
class DbClient:

    @classmethod
    def db_setting(cls,
                   db_setting_file_path: str):
        """

        :param db_setting_file_path:
        :return:
        """
        result = os.path.exists(db_setting_file_path)
        if result:
            f = open(db_setting_file_path, "r", encoding="utf-8")
            db_conn = yaml.safe_load(f); f.close()
            print(db_conn)
            return pymysql.connect(host= f"{db_conn['mariahost']}",
                                    user= db_conn["user"],
                                    password= db_conn["password"],
                                    db= db_conn["db"])
        else:
            raise FileNotFoundError

    @classmethod
    def data_insert(cls,
                    table:str, total_count: int, team_name: str, mariadb_client: pymysql.connect):
        """

        :param table:
        :param total_count:
        :param team_name:
        :param mariadb_client:
        :return:
        """
        col1 = "total_insert_size"
        col2 = "team_name"

        sql = f"INSERT INTO {table} (`{col1}`, `{col2}`) "+"VALUES (%s, %s)"
        print(sql)
        val = (total_count, team_name)

        mariadb_cursor = mariadb_client.cursor()
        mariadb_cursor.execute(sql, val)
        mariadb_client.commit()

    @classmethod
    def client_memory_free(cls,
                           mariadb_client: pymysql.connect):
        """

        :param mariadb_client:
        :return:
        """
        mariadb_client.close()
