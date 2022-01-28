import requests
import os
from bs4 import BeautifulSoup
from elasticsearch.helpers import bulk
import urllib.request as urlReq
from selenium.webdriver.common.by import By

from Team.CommonCode.common import Common
from Team.CommonCode.player_template import ret_player_template
from Elastic.es_client import ElasticClient
from seleniumObject.seleniumClient import ret_selenium_client
from MariadbClient.dbClient import DbClient
##
# 작성자 : 김준현
# 작성일 : 20220119
# 대상 : 흥국 생명
## ==========================
class PINK_SPIDER(Common, ElasticClient):

    def __init__(self):
        self.team_name = "pink_spider"
        Common.__init__(self, team_name=self.team_name)
        ElasticClient.__init__(self, self.es_config_path)

        self.url = "https://www.pinkspiders.co.kr/team/player_list.php"
        self.player_image_path = "static/image/pink_spider"

        self.selenium_client = ret_selenium_client()

    def get_data(self):
        """

        :return:
        """
        response = requests.get(self.url, verify=False)
        if response.status_code == 200:
            bs_object = BeautifulSoup(response.text, "html.parser")
            player_list = bs_object.select_one("section.team.player > "
                                               "div.title_sub > "
                                               "div.row_group.col_3.player_list").select("a.col")

            for a in player_list:
                ret_player_template_data = ret_player_template(team_name=self.team_name)
                href_url = "https://www.pinkspiders.co.kr/team" + str(a.attrs["href"]).lstrip(".")
                txt_g = a.select_one("spn.txt_g")
                player_position = str(txt_g.select_one("span.player_position").text).lower()
                player_number = str(txt_g.select_one("span.player_no").text).lstrip("NO.").lstrip("0")
                player_kor_name = txt_g.select_one("span.player_name").text
                print(player_kor_name)

                ret_player_template_data["player_position"] = player_position
                ret_player_template_data["player_number"] = int(player_number)
                ret_player_template_data["player_kor_name"] = player_kor_name

                self.detail_player(detail_player_url= href_url, ret_player_template_data=ret_player_template_data)

            self.total_count = len(self.element_list)

    def detail_player(self, detail_player_url: str, ret_player_template_data: dict) -> dict:
        """

        :param detail_player_url:
        :param ret_player_template_data:
        :return:
        """
        print(detail_player_url)
        self.selenium_client.get(detail_player_url)
        self.selenium_client.implicitly_wait(3)
        li_list = self.selenium_client.find_element(By.CLASS_NAME, "col.frame_g")\
            .find_element(By.TAG_NAME, "ul")\
            .find_elements(By.TAG_NAME, "li")

        for i in li_list:
            k,v = str(i.text).split(":")
            k = str(k).strip(); v = str(v).strip()

            if k == "생년월일":
                ret_player_template_data["player_birthday"] = v
                ret_player_template_data["player_age"] = int(self.year) - int(v.split(".")[0])
            elif k == "신장":
                ret_player_template_data["player_height"] = int(v.rstrip("cm"))
            elif k == "출신학교":
                if v:
                    player_school_list = v.split("-")
                    for sch in player_school_list:
                        if sch[-1] == "초":
                            ret_player_template_data["player_elementary_school"] = sch
                        elif sch[-1] == "중":
                            ret_player_template_data["player_middle_school"] = sch
                        elif sch[-1] == "고":
                            ret_player_template_data["player_high_school"] = sch

        img_url = self.selenium_client.find_element(By.CLASS_NAME, "img_group.col").find_element(By.TAG_NAME, "img").get_attribute("src")

        save_file_name = self.file_name.format(team_name=self.team_name,
                                               player_name=ret_player_template_data["player_kor_name"])

        save_file_path = os.path.join(os.path.join(self.img_save_path, self.player_image_path), save_file_name)
        urlReq.urlretrieve(img_url, save_file_path)
        ret_player_template_data["player_photo_image_path"] = f"/{self.player_image_path}/{save_file_name}"
        ## =======================
        ret_player_template_data["player_unique_key"] = self.es_primary_key.format(
            team_name=ret_player_template_data["player_team_name"],  # team name
            player_number=ret_player_template_data["player_number"],  # player back number
            player_name=ret_player_template_data["player_kor_name"]  # player name
        )
        ## ========================

        self.element_list.append(
            {
                "_index": self.es_indice,
                "_id": ret_player_template_data["player_unique_key"],
                "_source": ret_player_template_data
            }
        )

    def elastic_bulk_insert(self):
        """

        :return:
        """

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

if __name__ == "__main__":
    o = PINK_SPIDER()
    o.get_data()
    o.elastic_bulk_insert()
    o.db_data_insert()