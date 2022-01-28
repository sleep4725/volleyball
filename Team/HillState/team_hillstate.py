import os
import requests
from bs4 import BeautifulSoup
import urllib.request as urlReq
from urllib.parse import quote_plus
from elasticsearch.helpers import bulk

from Team.CommonCode.common import Common
from Team.CommonCode.player_template import ret_player_template

from Elastic.es_client import ElasticClient
from MariadbClient.dbClient import DbClient
##
# 작성자 : 김준현
# 작성일 : 20220119
# 대상 : 현대건설
## ==========================
class HillState(Common, ElasticClient):

    def __init__(self):
        Common.__init__(self, team_name="hill_state")
        ElasticClient.__init__(self, self.es_config_path)

        self.url = "https://hillstate.hdec.kr/Contents_Player/Player"
        self.player_image_path = "static/image/hill_state"

        self.player_team_name = "hill_state"

    def get_data(self):
        """

        :return:
        """

        response = requests.get(self.url)
        if response.status_code == 200:
            bs_object = BeautifulSoup(response.text, "html.parser")
            a_tag_list = bs_object.select("div.cover-wrap > a")

            for a in a_tag_list:
                href = f"https://hillstate.hdec.kr/{a.attrs['href']}"
                self.detail_player(url= href)

            self.total_count = len(self.element_list)

    def detail_player(self, url: str) -> dict:
        """

        :param url:
        :return:
        """
        response = requests.get(url)
        if response.status_code == 200:
            ret_player_template_data = ret_player_template(team_name=self.player_team_name)
            bs_object = BeautifulSoup(response.text, "html.parser")

            player_info = bs_object.select_one("div.player-info")
            player_position = str(player_info.select_one("div.position").text).lower()
            player_number = int(str(player_info.select_one("div.number").text).replace("\n", "").lstrip("No."))
            player_kor_name = player_info.select_one("div.name").text

            ret_player_template_data["player_position"] = player_position # 선수 포지션
            ret_player_template_data["player_number"] = player_number     # 선수 백넘버
            ret_player_template_data["player_kor_name"] = player_kor_name # 선수 이름

            li_list = bs_object.select_one("div.cont-area > ul").select("li.tpA")

            for l in li_list:

                if l.select_one("p.tit").text == "생년월일":
                    player_birthday = str(l.select_one("p.cont").text).\
                        replace(" ", "").\
                        replace("년", ".").\
                        replace("월", ".").\
                        replace("일", "")
                    ret_player_template_data["player_birthday"] = player_birthday
                    ret_player_template_data["player_age"] = int(self.year) - int(player_birthday.split(".")[0])

                elif l.select_one("p.tit").text == "출신학교":
                    school = [d.strip(" ") for d in str(l.select_one("p.cont").text).split("-")]
                    for i in school:
                        if i[-1] == "초":
                            ret_player_template_data["player_elementary_school"] = i
                        elif i[-1] == "중":
                            ret_player_template_data["player_middle_school"] = i
                        elif i[-1] == "고":
                            ret_player_template_data["player_high_school"] = i

                elif l.select_one("p.tit").text == "신장/체중":
                    ret_player_template_data["player_height"]= int(str(l.select_one("p.cont").text).split("/")[0].rstrip().rstrip("cm"))

            img_url_path_list = str(bs_object.select_one("div.player-img > img").attrs["src"]).lstrip("/").split("/")
            player_name = str(img_url_path_list[-1]).split("_")[0]
            player_name = quote_plus(player_name)
            img_url_path_list[2] = img_url_path_list[2].replace(ret_player_template_data["player_kor_name"],
                                                                player_name)
            img_url_path = "/".join(img_url_path_list)

            img_url = "https://hillstate.hdec.kr/" + img_url_path

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
                })

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
    o = HillState()
    o.get_data()
    o.elastic_bulk_insert()
    o.db_data_insert()