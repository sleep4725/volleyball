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
# 작성일 : 20220117
# 대상 : 한국도로공사
## ==========================
class Hypass(Common, ElasticClient):

    def __init__(self):
        self.team_name = "hypass"
        Common.__init__(self, team_name=self.team_name)
        ElasticClient.__init__(self, self.es_config_path)

        self.url = "https://www.exsportsclub.com/player/player/"
        self.player_image_path = "static/image/hypass"

        self.regex = "\(.*\)|\s-\s.*"
        self.player_position_data = ret_player_position()

    def get_data(self):
        """

        :return:
        """
        response = requests.get(self.url)
        if response.status_code == 200:
            bs_object = BeautifulSoup(response.text, "html.parser")
            for a in bs_object.select("div.wpb_wrapper > p > a"):
                href_url = "https://www.exsportsclub.com" + a.attrs["href"]
                self.detail_player(detail_player_url=href_url)

            self.elastic_bulk_insert()
            self.total_count = len(self.element_list)
            self.db_data_insert()

    def detail_player(self, detail_player_url: str):
        """

        :param detail_player_url:
        :return:
        """
        response = requests.get(detail_player_url)
        if response.status_code == 200:
            ret_player_template_data = ret_player_template(team_name=self.team_name)
            bs_object = BeautifulSoup(response.text, "html.parser")
            content_list = bs_object.select_one("div.wpb_wrapper > "
                                               "table.web-main-player-detail-table > "
                                               "tbody").select("tr")

            for c in content_list:
                data = [d.text for d in c.select("td")]
                key_list = data[0::2]
                value_list = data[1::2]
                for k, v in zip(key_list, value_list):
                    if k == "이름":
                        player_back_number = str(re.search(r'\((.*?)\)', v).group(1)).lstrip("NO.").lstrip("0")
                        player_name = re.sub(self.regex, "", v)

                        ret_player_template_data["player_number"] = player_back_number
                        ret_player_template_data["player_kor_name"] = player_name
                    elif k == "생년월일":
                        # 1989년 11월 30일
                        player_birthday = str(v).replace(" ", "").replace("년", ".").replace("월", ".").replace("일", "")
                        ret_player_template_data["player_birthday"] = player_birthday
                        player_age = int(self.year) - int(player_birthday.split(".")[0])
                        ret_player_template_data["player_age"] = player_age
                    elif k == "신장 / 체중":
                        # 182cm / 68kg
                        ret_player_template_data["player_height"] = int(str(v).split("/")[0].rstrip(" ").rstrip("cm"))
                    elif k == "포지션":
                        ret_player_template_data["player_position"] = self.player_position_data[v]
                    elif k == "출신학교":
                        school_3 = v[-3:]; school_4 = v[-4:]
                        if school_3 in ["대학교", "중학교"]:
                            if school_3 == "대학교":
                                ret_player_template_data["player_university"] = v
                            elif school_3 == "중학교":
                                ret_player_template_data["player_middle_school"] = v
                        elif school_4 == "고등학교":
                            ret_player_template_data["player_high_school"] = str(v).rstrip("고등학교").replace("여자", "여고")
                        else:
                            ret_player_template_data["player_university"] = v

            img_url = bs_object.select_one("div.vc_single_image-wrapper.vc_box_border_grey > img").attrs["src"]
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
            print(ret_player_template_data)

            self.element_list.append(
                {
                    "_index": self.es_indice,
                    "_id": ret_player_template_data["player_unique_key"],
                    "_source": ret_player_template_data
                })

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
            print(f"적재 성공: {self.total_count}")

h = Hypass()
h.get_data()