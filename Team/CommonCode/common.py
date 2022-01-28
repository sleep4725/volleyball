from abc import *
import time

from MariadbClient.dbClient import DbClient

class Common(metaclass= ABCMeta):

    def __init__(self, team_name):
        self.team_name = team_name # 팀이름
        self.file_name = "{team_name}_player_{player_name}.jpg"
        self.element_list = list()
        self.year = time.strftime("%Y", time.localtime())
        self.es_config_path = "../../Config/es/es_config.yml"
        self.es_primary_key = "{team_name}_{player_number}_{player_name}"
        self.total_count = 0
        self.table_name = "kor_girl_volleball"
        self.img_save_path = r"C:\Users\sleep\PycharmProjects\pythonGraphProj"
        self.conn = DbClient.db_setting(r"C:\Users\sleep\PycharmProjects\volleball\Config\mariadb\connect_inform.yml")

    @abstractmethod
    def db_data_insert(self):
        pass

    @abstractmethod
    def get_data(self):
        pass

    @abstractmethod
    def elastic_bulk_insert(self):
        pass