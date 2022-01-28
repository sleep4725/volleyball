import os
import requests
from bs4 import BeautifulSoup
import urllib.request as urlReq
from elasticsearch.helpers import bulk

from Team.CommonCode.common import Common
from Team.CommonCode.player_template import ret_player_template

from Elastic.es_client import ElasticClient
from MariadbClient.dbClient import DbClient

## ==========================
# 작성자 : 김준현
# 작성일 : 20220122
## ==========================
class AiPepper(Common, ElasticClient):

    def __init__(self):
        Common.__init__(self, team_name="ai_papper")
        ElasticClient.__init__(self, self.es_config_path)

        self.player_team_name = "ai_papper"
        self.url = "http://www.aipeppers.kr/team_sub1.php"
        self.player_image_path = "static/image/ai_papper"

    def get_data(self):
        """

        :return:
        """
        response = requests.get(self.url)
        if response.status_code == 200:
            bs_object = BeautifulSoup(response.text, "html.parser")
            team_sub1_section2 = bs_object.select_one("div#team_sub1_section2 > ul")
            result = team_sub1_section2.select("li")

            for indx, li in enumerate(result):
                img_tag = li.select_one("div > a > img")
                href_url= li.select_one("div > a")

                url = f"http://www.aipeppers.kr/{img_tag.attrs['src']}"
                href_url  = f"http://www.aipeppers.kr/{href_url.attrs['href']}"

                response_detail = requests.get(href_url)

                element = ret_player_template()

                if response_detail.status_code == 200:
                    bs_object_detail = BeautifulSoup(response_detail.text, "html.parser")
                    player_detail = bs_object_detail.select_one("div#team_sub1_section2 > "
                                                "div.player_detail > "
                                                "div.player_detail_info")

                    element["player_position"] = str(player_detail.select_one("h3").text).lower()
                    element["player_number"] = str(player_detail.select_one("h2").text).lstrip("No.")

                    # ===============================
                    # 초등학교 : elementary School
                    # 중학교 : middle School
                    # 고등학교 : high school
                    # ===============================
                    player_information = str(player_detail.select("span")[1].text).strip().replace(" ", "").split("\n")
                    player_information = [p.split(":") for p in player_information if p]
                    for k,v in player_information:
                        if k == "생년월일":
                            element["player_birthday"] = v
                            element["player_age"] = int(self.year) - int(str(v).split(".")[0])
                        elif k == "출신학교":
                            school = str(v).split("/")
                            element["player_elementary_school"] = school[0]
                            element["player_middle_school"] = school[1]
                            element["player_high_school"] = school[2]
                        elif k == "신장":
                            element["player_height"] = str(v).replace("cm", "")

                    player_name = player_detail.select_one("p").text
                    player_name = player_name.strip()
                    player_eng_name, player_kor_name = player_name.split("\n")
                    player_kor_name = str(player_kor_name).lstrip(" ")

                    element["player_kor_name"] = player_kor_name
                    element["player_eng_name"] = player_eng_name

                save_file_name = self.file_name.format(team_name=self.team_name,
                                                       player_name=element["player_kor_name"])

                save_file_path = os.path.join(os.path.join(self.img_save_path, self.player_image_path), save_file_name)

                urlReq.urlretrieve(url, save_file_path)
                element["player_photo_image_path"] = f"/{self.player_image_path}/{save_file_name}"
                element["player_team_name"] = self.player_team_name
                element["player_unique_key"] = self.es_primary_key.format(
                    team_name=element["player_team_name"],  # team name
                    player_number=element["player_number"],  # player back number
                    player_name=element["player_kor_name"]  # player name
                )

                self.element_list.append({
                    "_index": self.es_indice,
                    "_id": element["player_unique_key"],
                    "_source": element
                })

            self.total_count = len(self.element_list)

    def data_print(self):
        """

        :return:
        """
        print(self.element_list)

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
            print("적재 성공")

if __name__ == "__main__":
    o = AiPepper()
    o.get_data()
    o.elastic_bulk_insert()
    o.db_data_insert()