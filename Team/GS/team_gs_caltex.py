import os
import requests
from bs4 import BeautifulSoup
import urllib.request as urlReq
from elasticsearch.helpers import bulk

from Team.CommonCode.common import Common
from Team.CommonCode.player_template import ret_player_template

from Elastic.es_client import ElasticClient
from MariadbClient.dbClient import DbClient
##
# 작성자 : 김준현
# 작성일 : 20220104
## ==========================
class GS(Common, ElasticClient):

    def __init__(self):
        Common.__init__(self, team_name="gs_caltex")
        ElasticClient.__init__(self, self.es_config_path)

        self.url = "https://www.gsvolleyball.com/team/player"
        self.player_image_path = "static/image/gs_caltex"

        self.player_team_name = "gs_caltext"

    def get_data(self):
        """

        :return:
        """
        response = requests.get(self.url)
        if response.status_code == 200:
            bs_object = BeautifulSoup(response.text, "html.parser")
            team_list = bs_object.select_one("ul.teamListUl")

            team_list = team_list.select("li")

            for li in team_list:
                url = f'https://www.gsvolleyball.com{li.select_one("a").attrs["href"]}'
                player_information = self.detail_player(url=url)
                url = li.select_one("a > div.pPhotoWrap > img").attrs["src"]
                save_file_name = self.file_name.format(team_name= self.team_name, player_name= player_information["player_kor_name"])

                save_file_path = os.path.join(os.path.join(self.img_save_path,self.player_image_path), save_file_name)

                urlReq.urlretrieve(url, save_file_path)
                player_information["player_photo_image_path"] = f"/{self.player_image_path}/{save_file_name}"
                player_information["player_team_name"] = self.player_team_name
                player_information["player_unique_key"] = self.es_primary_key.format(
                    team_name=player_information["player_team_name"],  # team name
                    player_number=player_information["player_number"],  # player back number
                    player_name=player_information["player_kor_name"]  # player name
                )
                self.element_list.append(
                    {
                        "_index": self.es_indice,
                        "_id": player_information["player_unique_key"],
                        "_source": player_information
                    })

            self.total_count = len(self.element_list)
            self.elastic_bulk_insert()

        self.db_data_insert()

    def detail_player(self, url: str)-> dict:
        """

        :param url:
        :return:
        """
        '''
            "player_kor_name": "",  # 선수 한글 이름
            "player_eng_name": "",  # 선수 영문 이름
            "player_position": "",  # 선수 포지션
            "player_number": "",  # 선수 백넘버
            "player_birthday": "", # 선수 생년월일
            "player_height": 0, # 선수 키
            "player_elementary_school": "",
            "player_middle_school": "",
            "player_high_school": "",
            "player_age": 0,
            "player_photo_image_path": ""
        '''
        ret_player_template_data = ret_player_template(team_name= self.player_team_name)
        response = requests.get(url)
        if response.status_code == 200:
            bs_object = BeautifulSoup(response.text, "html.parser")
            detail_info = bs_object.select_one("div.tDetailInfo")

            div_information_list = detail_info.select_one("div > div.tdi_1").select("div")
            tdi_2 = detail_info.select_one("div > div.tdi_2")
            tdi_3 = detail_info.select_one("div > div.tdi_3")
            player_kor_name = tdi_2.select_one("strong").text
            player_eng_name = tdi_2.select_one("span").text
            tdi_3_dl = tdi_3.select("dl")

            for t in tdi_3_dl:
                if t.select_one("dt").text == "생년월일":
                    # 1989년 02월 01일
                    player_birthday = t.select_one("dd").text
                    player_birthday_year = str(player_birthday).split(" ")[0].rstrip("년")
                    ret_player_template_data["player_birthday"] = str(player_birthday).replace(" ", "").\
                        replace("년", ".").\
                        replace("월", ".").\
                        replace("일", "")

                    ret_player_template_data["player_age"] = int(self.year) - int(player_birthday_year)

                elif t.select_one("dt").text == "출신교":
                    school = [d.strip(" ") for d in str(t.select_one("dd").text).split("-")]

                    for i in school:
                        if i[-1] == "초":
                            ret_player_template_data["player_elementary_school"] = i
                        elif i[-1] == "중":
                            ret_player_template_data["player_middle_school"] = i
                        elif i[-1] == "고":
                            ret_player_template_data["player_high_school"] = i
                        else:
                            ret_player_template_data["player_school"] = i

                elif t.select_one("dt").text == "신장":
                    ret_player_template_data["player_height"]=int(str(t.select_one("dd").text).replace("cm", ""))

            player_backnumber = div_information_list[0].select_one("dl > dd.tDetailNumber")
            player_position = div_information_list[1].select_one("dl > dd.tDetailPosition")

            ret_player_template_data["player_number"] = player_backnumber.text
            ret_player_template_data["player_position"] = str(player_position.text).lower()
            ret_player_template_data["player_kor_name"] = player_kor_name
            ret_player_template_data["player_eng_name"] = player_eng_name

        return ret_player_template_data

    def elastic_bulk_insert(self):
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
    o = GS()
    o.get_data()
