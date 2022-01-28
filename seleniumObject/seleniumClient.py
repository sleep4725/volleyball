from selenium import webdriver

## ==============================
# 작성자 : 김준현
# 작성일 : 2022-01-13
## ==============================
def ret_selenium_client()-> webdriver.Chrome:
    """
    :return:
    """
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    options.add_argument('window-size=1920x1080')
    options.add_argument("disable-gpu")

    # UserAgent값을 바꿔줍시다!
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36")

    driver = webdriver.Chrome(r'C:\Users\sleep\PycharmProjects\volleball\SeleniumDriver\chromedriver.exe', chrome_options=options)

    return driver