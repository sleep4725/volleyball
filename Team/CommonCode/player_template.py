def ret_player_template(team_name="")-> dict:
    """

    :return:
    """
    e = {
        "player_kor_name": "",  # 선수 한글 이름
        "player_eng_name": "",  # 선수 영문 이름
        "player_position": "",  # 선수 포지션
        "player_number": "",  # 선수 백넘버
        "player_birthday": "", # 선수 생년월일
        "player_height": 0, # 선수 키
        "player_elementary_school": "", # 선수 초등학교
        "player_middle_school": "", # 선수 중학교
        "player_high_school": "",   # 선수 고등학교
        "player_university": "", # 선수 대학교
        "player_age": 0, # 선수 나이
        "player_photo_image_path": "",
        "player_unique_key": "",
        "player_team_name": team_name
    }

    return e