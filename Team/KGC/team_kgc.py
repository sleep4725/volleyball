import requests
import os
from bs4 import BeautifulSoup
from elasticsearch.helpers import bulk
import urllib.request as urlReq
from selenium.webdriver.common.by import By

from Team.CommonCode.common import Common
from Team.CommonCode.player_template import ret_player_template
from Elastic.es_client import ElasticClient
from MariadbClient.dbClient import DbClient
##
# 작성자 : 김준현
# 작성일 : 20220117
# 대상 : KGC 인삼공사
## ==========================
class KGC(Common, ElasticClient):

    def __init__(self):
        self.team_name = "kgc"
        Common.__init__(self, team_name=self.team_name)
        ElasticClient.__init__(self, self.es_config_path)

        self.url = "https://www.kgcsports.com/volleyball/player/player_list.php"
        self.player_image_path = "static/image/kgc"


    def get_data(self):
        """

        :return:
        """
        response = requests.get(self.url)
        if response.status_code == 200:
            bs_object = BeautifulSoup(response.text, "html.parser")
            player_list = bs_object.select_one("div#contents").select("div.plist")
            for p in player_list:
                player_position = p.select_one("dl > dt").text

                player_list = p.select_one("dl > dd > ul").select("li")

                for li in player_list:
                    ret_player_template_data = ret_player_template(team_name=self.team_name)
                    ret_player_template_data["player_position"] = str(player_position).lower()  # 1번 - 선수 포지션
                    bg_name = li.select_one("a > span.bg_name")
                    player_number = bg_name.select_one("em").text
                    player_kor_name = bg_name.select_one("strong").text
                    ret_player_template_data["player_number"] = str(player_number).lstrip("No.")  # 2번 - 선수 배번
                    ret_player_template_data["player_kor_name"] = player_kor_name # 3번 - 선수 이름
                    a_tag = li.select_one("a")
                    href = "https://www.kgcsports.com/volleyball/player/" + a_tag.attrs["href"]

                    img_url = "https://www.kgcsports.com/" + a_tag.select_one("img").attrs["src"]

                    save_file_name = self.file_name.format(team_name=self.team_name,
                                                           player_name=ret_player_template_data["player_kor_name"])

                    save_file_path = os.path.join(os.path.join(self.img_save_path,self.player_image_path), save_file_name)
                    urlReq.urlretrieve(img_url, save_file_path)

                    ret_player_template_data["player_photo_image_path"] =  f"/{self.player_image_path}/{save_file_name}"
                    ret_player_template_data["player_unique_key"] = self.es_primary_key.format(
                        team_name= ret_player_template_data["player_team_name"],  # team name
                        player_number= ret_player_template_data["player_number"], # player back number
                        player_name= ret_player_template_data["player_kor_name"]  # player name
                    )

                    self.detail_player(detail_player_url= href, ret_player_template_data= ret_player_template_data)
                    self.element_list.append(
                        {
                            "_index": self.es_indice,
                            "_id": ret_player_template_data["player_unique_key"],
                            "_source": ret_player_template_data
                        }
                    )

                self.total_count += len(self.element_list)
                self.elastic_bulk_insert()
                self.element_list.clear()

            self.db_data_insert()

    def detail_player(self, detail_player_url: str, ret_player_template_data: dict) -> dict:
        """

        :param detail_player_url:
        :return:
        """
        response = requests.get(detail_player_url)
        if response.status_code == 200:
            bs_object = BeautifulSoup(response.text, "html.parser")
            player_profile = bs_object.select_one("div.player_profile > ul.player_detail").select("li")

            for i in player_profile:

                if i.select_one("em").text == "생년월일":
                    player_birthday = str(i.text).lstrip("생년월일")
                    ret_player_template_data["player_birthday"] = player_birthday
                    player_age = int(self.year) - int(player_birthday.split(".")[0])
                    ret_player_template_data["player_age"] = player_age

                elif i.select_one("em").text == "출신학교":
                    player_school = str(i.text).lstrip("출신학교")
                    if player_school != "-":
                        player_school_list = [s.strip() for s in player_school.split("-")]
                        for sch in player_school_list:
                            if sch[-1] == "초":
                                ret_player_template_data["player_elementary_school"] = sch
                            elif sch[-1] == "중":
                                ret_player_template_data["player_middle_school"] = sch
                            elif sch[-1] == "고":
                                ret_player_template_data["player_high_school"] = sch

                elif i.select_one("em").text == "신장":
                    player_height = str(i.text).lstrip("신장").rstrip(" \n").rstrip("cm")
                    ret_player_template_data["player_height"] = player_height


    def elastic_bulk_insert(self):
        try:

            bulk(self.es_client, self.element_list)
        except:
            pass
        else:
            print(f"전체 {len(self.element_list)} 건 적재 완료")

    def db_data_insert(self):
        """

        :return:
        """
        try:

            DbClient.data_insert(table=self.table_name,
                                 total_count= self.total_count,
                                 team_name= self.team_name,
                                 mariadb_client= self.conn)
        except:
            pass
        else:
            print("적재 성공")

k = KGC()
k.get_data()
