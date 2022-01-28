import requests
import os
import re
from bs4 import BeautifulSoup
from elasticsearch.helpers import bulk
import urllib.request as urlReq
from selenium.webdriver.common.by import By

from Team.CommonCode.common import Common
from Team.CommonCode.player_template import ret_player_template
from Team.CommonCode.player_position import ret_player_position
from Elastic.es_client import ElasticClient
from MariadbClient.dbClient import DbClient
##
# 작성자 : 김준현
# 작성일 : 20220118
# 대상 : IBK
## ==========================
class IBK(Common, ElasticClient):

    def __init__(self):
        self.team_name = "ibk"
        Common.__init__(self, team_name=self.team_name)
        ElasticClient.__init__(self, self.es_config_path)

        self.url = "http://sports.ibk.co.kr/volleyball/team/player_list.php"
        self.player_image_path = "static/image/ibk"

    def get_data(self):
        """

        :return:
        """
        response = requests.get(self.url)
        if response.status_code == 200:
            bs_object = BeautifulSoup(response.text, "html.parser")
            contents = bs_object.select_one("div#container > div#contents")
            player_position = str(contents.select_one("h3.h3_tit").text).lower()

            plist = contents.select("div.plist")

            for p in plist:
                href_list = ["http://sports.ibk.co.kr/volleyball/team" + str(l.select_one("a").attrs["href"]).lstrip(".")
                             for l in p.select_one("ul").select("li")]

                self.detail_player(detail_player_url_list=href_list, player_position=player_position)

            self.elastic_bulk_insert()
            self.db_data_insert()

    def detail_player(self, detail_player_url_list: list, player_position: str):
        """

        :param detail_player_url_list:
        :return:
        """
        for u in detail_player_url_list:
            response = requests.get(u)
            if response.status_code == 200:
                ret_player_template_data = ret_player_template(team_name=self.team_name)
                ret_player_template_data["player_position"] = player_position

                bs_object = BeautifulSoup(response.text, "html.parser")
                ## ----------------------------------------------------
                img_url = "http://sports.ibk.co.kr" + bs_object.select_one("div.pic > img").attrs["src"]
                ## ----------------------------------------------------
                player_profile = bs_object.select_one("div.profile")
                player = player_profile.select_one("div.pname").text
                player_number = re.sub(r'[^0-9]', '', player)
                ret_player_template_data["player_number"] = player_number
                ret_player_template_data["player_kor_name"] = player.lstrip(player_number)

                li_list = player_profile.select_one("div.pro_list > ul").select("li")

                for l in li_list:
                    k, v= str(l.text).split(":")
                    k = str(k).strip()
                    v = str(v).strip()

                    if k == "생년월일":
                        v = v.replace(" ", "").replace("년", ".").replace("월", ".").replace("일", "")
                        ret_player_template_data["player_birthday"] =v
                        ret_player_template_data["player_age"] =int(self.year) - int(v.split(".")[0])
                    elif k == "출신학교":
                        if v != "-":
                            player_school = v.replace(" ", "").split("-")
                            for sch in player_school:
                                print(sch)
                                if sch[-1] == "초":
                                    ret_player_template_data["player_elementary_school"] = sch
                                elif sch[-1] == "중":
                                    ret_player_template_data["player_middle_school"] = sch
                                elif sch[-1] == "고":
                                    ret_player_template_data["player_high_school"] = sch
                                elif sch[-1] == "대":
                                    ret_player_template_data["player_university"] = sch
                    elif k == "신장":
                        ret_player_template_data["player_height"] = int(v.rstrip("cm"))

                save_file_name = self.file_name.format(team_name=self.team_name,
                                                       player_name=ret_player_template_data["player_kor_name"])

                save_file_path = os.path.join(os.path.join(self.img_save_path,self.player_image_path), save_file_name)
                urlReq.urlretrieve(img_url, save_file_path)
                ret_player_template_data["player_photo_image_path"] = f"/{self.player_image_path}/{save_file_name}"
                ret_player_template_data["player_unique_key"] = self.es_primary_key.format(
                    team_name=ret_player_template_data["player_team_name"],  # team name
                    player_number=ret_player_template_data["player_number"],  # player back number
                    player_name=ret_player_template_data["player_kor_name"]  # player name
                )

                self.element_list.append(
                    {
                        "_index": self.es_indice,
                        "_id": ret_player_template_data["player_unique_key"],
                        "_source": ret_player_template_data
                    }
                )

        self.total_count = len(self.element_list)

    def elastic_bulk_insert(self):
        """

        :return:
        """

        try:

            bulk(self.es_client, self.element_list)
        except:
            pass


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
            print(f"적재 성공: {self.total_count}")

if __name__ == "__main__":
    o = IBK()
    o.get_data()