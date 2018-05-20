# coding=utf-8
import csv
import calendar
import datetime
import json
import random

import itertools
import requests

from bs4 import BeautifulSoup
from janome.tokenizer import Tokenizer
from requests_oauthlib import OAuth1Session
from summpy.lexrank import summarize
from google.cloud import bigquery
from google.oauth2 import service_account


# twitterAPI
oath_key_dict = {
    "consumer_key": "2qimKikZwCOJXG0wxJ0lzkcM6",
    "consumer_secret": "MHAjJsYvGCF0mVkgs9w0tJh0fJf0ZpBMKqUMiqTUzQmqYoIFA2",
    "access_token": "157729228-r5JXs6Mi79rEgPAd1AyS9w5l7BaUADzrmLpc9JiR",
    "access_token_secret": "Dm0C0ZPCBCDcNARnAaJvUDxEk88o1pbTtWuZgvILzFG2u"
}

research_ids = ["get2ch_soccer", "BaseballNEXT", "gorin"]
pattern = r"(https?|ftp)(:\/\/[-_\.!~*\'()a-zA-Z0-9;\/?:\@&=\+\$,%#]+)"
rss_news = [r"https://headlines.yahoo.co.jp/rss/jsportsv-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/soccerk-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/bfj-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/nallabout-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/asahik-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/baseballk-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/spnaviv-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/tennisnet-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/nksports-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/gekisaka-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/fullcount-c_spo.xml"]

json_key = 'sports-agent-9947da5a2148.json'
client = bigquery.Client.from_service_account_json(json_key, project='deep-equator-204407')

r_base_url_baseball = [r'https://baseball.yahoo.co.jp/npb/game/']
r_prop = [r'/text', r'/stats']
r_base_url_soccer = [r'https://soccer.yahoo.co.jp/jleague/game/live/']

player_record = {}
months = {}
for i, v in enumerate(calendar.month_abbr):
    months[v] = i


def create_oath_session(oath_key_dict):
    oath = OAuth1Session(
        oath_key_dict["consumer_key"],
        oath_key_dict["consumer_secret"],
        oath_key_dict["access_token"],
        oath_key_dict["access_token_secret"]
    )
    return oath


class SportsLive:
    def __init__(self, parent=None):
        pass

    '''
    形態素解析
    '''
    @staticmethod
    def morphological_analysis(text):
        txt = text
        t = Tokenizer()
        word_dic = {}
        lines = txt.split("\r\n")
        for line in lines:
            blog_txt = t.tokenize(line)
            for w in blog_txt:
                word = w.surface
                ps = w.part_of_speech
                if ps.find('名詞') < 0:
                    continue
                if word not in word_dic:
                    word_dic[word] = 0
                word_dic[word] += 1

        keys = sorted(word_dic.items(), key=lambda x: x[1], reverse=True)
        keyword = ''
        for word, cnt in keys[:4]:
            print("{0} ".format(word))
            keyword += "{0} ".format(word)

        return keyword

    """v1 direct score getting"""
    def score_check(self, keyword):
        data = []

        try:
            target_url = 'https://sports.yahoo.co.jp/search/text?query=' + keyword
            resp = requests.get(target_url)
            soup = BeautifulSoup(resp.text, "html.parser")

            tables = soup.find_all("p", class_="siteUrl")

            for table in tables:
                geturl = table.text
                geturl = geturl.rstrip(' － キャッシュ')

                data.append(geturl)
        except:
            pass
        score = ''

        try:
            for url in data:
                if 'game' in url:
                    score = self.get_score(url)
                    break
                else:
                    continue

        except:
            pass

        return score

    """v1 direct twitter getting"""
    def twitter_check(self, keyword, debug=False):
        keyword_list = keyword.split(' ')
        tweet_list = []
        output_list = []
        json_dict = {}

        for keyword in keyword_list:
            if keyword == "":
                break

            for research_id in research_ids:
                tweets = self.tweet_search(keyword, oath_key_dict, research_id)

                for tweet in tweets["statuses"]:
                    text = tweet['text']
                    text = self.tweet_analysis(text)
                    if not text[0] in outtext:
                        outtext += text[0] + '<br>'

                outtext2 += outtext[:600]
                outtext = ''

            outtext2 = outtext2.replace(keyword, '<font color="red">' + keyword + '</font>')

        return outtext2

    def news_check(self, keyword, debug=False):
        news_dict = {}
        keyword = keyword.split(' ')
        output_text = ""
        json_dict = {}

        for rss in rss_news:
            resp = requests.get(rss)
            soup = BeautifulSoup(resp.text, "html.parser")

            titles = soup.find_all("title")
            links = soup.find_all("link")

            for title, link in zip(titles, links):
                news_dict.update({title.next: str(link.next).replace('\n', '').replace(' ', '')})

        for key in keyword:
            if key == "":
                break

            news_key_list = [l for l in news_dict.keys() if key in l]

            for list_key in news_key_list:
                text = ""
                resp = requests.get(news_dict[list_key])
                soup = BeautifulSoup(resp.text, "html.parser")

                for s in soup.find_all("p", class_="ynDetailText"):
                    text += s.get_text()
                analysis_text = self.tweet_analysis(text)

                if debug:
                    # タイトル：｛リンク，全文，要約｝
                    json_dict.update({list_key:
                    {
                        'link':news_dict[list_key],
                        'text':text,
                        'a_text':analysis_text,
                    }}
                    )

                output_text += '<br>'.join(analysis_text)

        json_dict.update({"result_text":output_text})

        encode_json_data = json.dumps(json_dict)

        return encode_json_data

    """v2 news server loading"""
    def news_loader(self, keyword, rowcount, day, debug=False):
        myquery = ""
        news_dict = {}
        output_text = ""
        rowcount_str = ""
        json_dict = {}         

        if 1 <= rowcount < 5:
            rowcount_str = "row{}_text".format(str(rowcount))
        else:
            rowcount_str = "Full_text"

        if debug and rowcount_str == "Full_text":
            myquery = """
                        SELECT Full_text as text,title,Full_text FROM sportsagent.newsrecord
                        WHERE title like '%{1}%' AND _PARTITIONTIME = TIMESTAMP('{0}')
                      """.format(day, str(keyword))
        elif debug:
            myquery = """
                        SELECT {0} as text,title,Full_text FROM sportsagent.newsrecord
                        WHERE title like '%{2}%' AND _PARTITIONTIME = TIMESTAMP('{1}')
                      """.format(rowcount_str, day, str(keyword))
        else:
            myquery = """
                        SELECT title as text, {0} FROM sportsagent.newsrecord
                        WHERE title like '%{2}%' AND _PARTITIONTIME = TIMESTAMP('{1}')
                      """.format(rowcount_str, day, str(keyword))
        try:
            query_job = client.query(myquery)
            results = query_job.result()  # Waits for job to complete.
            result_list = list(results)
        except:
            raise NameError(myquery)
        
        try:
            if 1 <= rowcount < 5:
                # random select for results
                randindex = random.randint(0, len(result_list) - 1)
                output_text = result_list[randindex][0]
            else:
                text = "".join([re.text for re in result_list])
                output_text = self.analsys_text(text, rowcount)
        except:
            raise NameError("get errors?")

        json_dict = {"speech": output_text,
                     "displayText": output_text,
                     "source": "apiai-news"}

        return json_dict

    """v2 player server loading"""
    @staticmethod
    def player_loader(keyword, day, debug=False):
        news_dict = {}
        output_text = ""

        if debug:
            myquery = """
                        SELECT name,record as text
                        FROM sportsagent.bplayerrecord
                        WHERE name like '%{1}%' AND _PARTITIONTIME = TIMESTAMP('{0}')
                      """.format(day, str(keyword))
        else:
            myquery = """
                        SELECT name,record as text
                        FROM sportsagent.bplayerrecord
                        WHERE name like '%{1}%' AND _PARTITIONTIME = TIMESTAMP('{0}')
                      """.format(day, str(keyword))

        query_job = client.query(myquery)
        results = query_job.result()  # Waits for job to complete.
        result_list = list(results)
        
        output_text = str(result_list[0][0]) + "は" + str(result_list[0][1]) + "でした"

        json_dict = {"speech": output_text,
                     "displayText": output_text,
                     "source": "apiai-player"}

        return json_dict

    @staticmethod
    def tweet_search(search_word, oath_key_dict, account):
        url = "https://api.twitter.com/1.1/search/tweets.json?"
        params = {
            "q": search_word,
            "from":account,
            "lang": "ja",
            "result_type": "recent",
            "count": "100"
        }

        oath = create_oath_session(oath_key_dict)
        responce = oath.get(url, params=params)
        if responce.status_code != 200:
            print("Error code: %d" % (responce.status_code))
            return None
        tweets = json.loads(responce.text)

        return tweets

    @staticmethod
    def get_score(url):
        target_url = url
        resp = requests.get(target_url)
        soup = BeautifulSoup(resp.text)

        if 'baseball' in url:
            score_table = soup.find('table', {'width': "100%", 'cellpadding': "0", 'cellspacing': "0", 'border': "0"})
            rows = score_table.findAll("tr")
            score = []
            text = '最新の試合の結果は' + '\n'

            try:
                for row in rows:
                    csvRow = []
                    for cell in row.findAll(['td', 'th']):
                        csvRow.append(cell.get_text())
                    score.append(csvRow)

                    text += '\t|'.join(csvRow) + '\n'

            finally:
                return text

        elif 'soccer' in url:
            hometeam = soup.find_all('div', class_="homeTeam team")
            hometotal = soup.find_all("td", class_="home goal")
            home1st = soup.find_all("td", class_="home first")
            home2nd = soup.find_all("td", class_="home second")
            awayteam = soup.find_all('div', class_="awayTeam team")
            awaytotal = soup.find_all("td", class_="away goal")
            away1st = soup.find_all("td", class_="away first")
            away2nd = soup.find_all("td", class_="away second")

            for homename, awayname, homegoal, awaygoal in zip(hometeam, awayteam, hometotal, awaytotal):
                text = '最新の試合の結果は' + '\n' + str(homename.text.replace('\n', '')) + \
                       '-' + str(awayname.text.replace('\n', '')) + '\n'

                if len(home1st[0].text) > -1:
                    text += home1st[0].text + '前半' + away1st[0].text + '\n'

                if len(home2nd[0].text) > -1:
                    text += home2nd[0].text + '後半' + away2nd[0].text + '\n'

                if len(homegoal) > -1:
                    text += homegoal.text + ' - ' + awaygoal.text

                return text

    @staticmethod
    def tweet_analysis(text):
        sentences, debug_info = summarize(
            text, sent_limit=5, continuous=True, debug=True
        )

        return sentences

    @staticmethod
    def analsys_text(text, rowcount):
        sentences, debug_info = summarize(
            text, sent_limit=rowcount, continuous=True, debug=True
        )

        return sentences

    @staticmethod
    def summarized(text, rowcount):
        json_dict = {}
        sentences, debug_info = summarize(
            text, sent_limit=rowcount, continuous=True, debug=True
        )
        
        output_text = " ".join(sentences)
        json_dict.update({"result_text": output_text})
        encode_json_data = json.dumps(json_dict)

        return encode_json_data

    @staticmethod
    def execute_sql(day, keyword, table, keyfield, fields, debug=False):
        news_dict = {}
        output_text = ""

        if type(fields) is list:
            field = ",".join(fields)

        myquery = """
                    SELECT TOP 1 {4}
                    FROM sportsagent.{2}
                    WHERE {3} like '%{1}%' AND _PARTITIONTIME = TIMESTAMP('{0}')
                    ORDER BY TIME AS DESC
                  """.format(day, keyword, table, keyfield, field)

        query_job = client.query(myquery)
        results = query_job.result()  # Waits for job to complete.
        result_list = list(results)

        output_text = str(result_list[0][0]) + "は" + str(result_list[0][1]) + "でした"

        json_dict = {"speech": output_text,
                     "displayText": output_text,
                     "source": "apiai-player"}

        return json_dict


@staticmethod
def execute_sql2(day, keywords, table, keyfields, fields, debug=False):
    news_dict = {}
    output_text = ""
    where = ""

    if type(fields) is list:
        field = ",".join(fields)

    for f,k in zip(keyfields,keywords):
        where += "{0} like '%{1}%' AND".format(f, k)

    myquery = """
                SELECT TOP 1 {3}
                FROM sportsagent.{2}
                WHERE {1} _PARTITIONTIME = TIMESTAMP('{0}')
                ORDER BY TIME AS DESC
              """.format(day, where, table, field)

    query_job = client.query(myquery)
    results = query_job.result()  # Waits for job to complete.
    result_list = list(results)

    output_text = str(result_list[0][0]) + "は" + str(result_list[0][1]) + "でした"

    json_dict = {"speech": output_text,
                 "displayText": output_text,
                 "source": "apiai-player"}

    return json_dict


class RecordAccumulation:
    def __init__(self):
        pass

    @staticmethod
    def save_csv(table, filename):
        with open(filename, "w", encoding="utf-8", newline='') as f:
            writer = csv.writer(f)
            for row in table:
                writer.writerow(row)

    """v2 current"""
    @staticmethod
    def get_jp_bplayer_record(date):
        rec_list = [["name", "type", "date", "time", "record"]]
        rec_tuple = []

        i = 1
        strdate = date.strftime('%Y%m%d')

        while True:
            # URL構築
            req = requests.get(r_base_url_baseball[0] +
                               strdate +
                               str(i).zfill(2) +
                               r_prop[1])

            if req.status_code != 200:
                break

            try:
                soup = BeautifulSoup(req.text, "lxml")
            except:
                soup = BeautifulSoup(req.text, "html.parser")

            # バッターの成績
            try:
                tables = soup.findAll("table", class_="yjS")

                for table in tables:
                    trlist = table.findAll("tr")
                    for tr in trlist:
                        if "位" in tr.text or "合計" in tr.text:
                            continue
                        td = tr.findAll("td")

                        record = [td[1].string,
                                  "b",
                                  strdate,
                                  datetime.datetime.now().strftime('%H%M%S'),
                                  td[3].string + "打数" +
                                  td[5].string + "安打 " +
                                  td[4].string + "得点で打率は" +
                                  td[2].string + "です。"]
                        rec_list.append(record)
                        rec_tuple.append(tuple(record))
            except:
                continue

            # ピッチャーの成績
            try:
                divs = soup.findAll("div", class_="pitcher")

                for div in divs:
                    trlist = div.findAll("tr")
                    for tr in trlist:
                        if "防御率" in tr.text:
                            continue
                        td = tr.findAll("td")

                        record = [td[1].string,
                                  "p",
                                  strdate,
                                  datetime.datetime.now().strftime('%H%M%S'),
                                  td[3].string + "投球回" +
                                  td[4].string + "投球数で" +
                                  "被安打が" + td[6].string +
                                  td[8].string + "奪三振しています。" +
                                  "防御率は" + td[2].string + "です。"]
                        rec_list.append(record)
                        rec_tuple.append(tuple(record))
            except:
                continue

            i += 1

        return rec_list, rec_tuple

    """v2 current"""
    def news_check(self, date):
        news_dict = {}
        output_text = ""
        news_list = [["title", "url", "Full_text", "row1_text", "row2_text", "row3_text", "row4_text", "time"]]
        news_tuple = []

        try:
            for rss in rss_news:
                resp = requests.get(rss)
                soup = BeautifulSoup(resp.text, "html.parser")

                items = soup.find_all("item")

                for item in items:
                    title = item.find_all("title")[0]
                    link = item.find_all("link")[0]
                    day = item.find_all("pubdate")[0].text

                    news_date = day.split(" ")
                    news_date = datetime.date(int(news_date[3]),
                                              int(months[news_date[2]]),
                                              int(news_date[1]))
                    if date == news_date:
                        news_dict.update({title.text: str(link.next).replace('\n', '').replace(' ', '')})

            news_key_list = [l for l in news_dict.keys()]

            for list_key in news_key_list:
                news = [str(list_key), str(news_dict[list_key])]
                if "(" in list_key:
                    n_title = list_key[0:list_key.index("(")]
                    news[0] = n_title
                text = ""
                resp = requests.get(news_dict[list_key])
                soup = BeautifulSoup(resp.text, "html.parser")

                for s in soup.find_all("p", class_="ynDetailText"):
                    text += s.get_text()

                news.append(text)
                for r_count in range(1, 5):
                    analysis_text = self.summarized(text, r_count)
                    output_text = ''.join(analysis_text)
                    news.append(str(output_text))
                    news.append(datetime.datetime.now().strftime('%H%M%S'))

                news_list.append(news)
                tnews = tuple(news)
                news_tuple.append(tnews)
        except:
            raise NameError("get errors?")
        return news_list, news_tuple

    """v2 current"""
    @staticmethod
    def get_jp_s_score(date):
        rec_list = [["team1", "team2", "date", "time", "score"]]
        rec_tuple = []
        record = []

        i = 1
        strdate = date.strftime('%Y%m%d')

        while True:
            # URL構築
            req = requests.get(r_base_url_soccer[0] +
                               strdate +
                               str(i).zfill(2))

            if req.status_code != 200:
                break

            try:
                soup = BeautifulSoup(req.text, "lxml")
            except:
                soup = BeautifulSoup(req.text, "html.parser")

            # チーム名取得
            try:
                div = soup.findAll("div", class_="name")

                for d in div:
                    record.append(d.string)
            except:
                continue

            record.append(strdate)
            record.append(datetime.datetime.now().strftime('%H%M%S'))

            # の成績
            try:
                td_home = soup.findAll("td", class_="home goal")
                td_away = soup.findAll("td", class_="away goal")
                record.append(td_home.string + "-" + td_away.string)
            except:
                continue
            rec_list.append(record)
            rec_tuple.append(tuple(record))

            i += 1

        return rec_list, rec_tuple

    """v2 current"""
    @staticmethod
    def get_jp_b_score(date):
        rec_list = [["team1", "team2", "date", "time", "score"]]
        rec_tuple = []
        record = []
        names = []
        score = []

        i = 1
        strdate = date.strftime('%Y%m%d')

        while True:
            # URL構築
            req = requests.get(r_base_url_baseball[0] +
                               strdate +
                               str(i).zfill(2) +
                               r_prop[0])

            if req.status_code != 200:
                break

            try:
                soup = BeautifulSoup(req.text, "lxml")
            except:
                soup = BeautifulSoup(req.text, "html.parser")

            # チーム名取得
            try:
                trs = soup.findAll("tr", class_="yjMS")

                for tr in trs:
                    b_tag = tr.findAll("b")
                    names.append(b_tag.string)
                    td = tr.findAll("td", class_="sum")
                    score.append(td.string)

            except:
                continue

            record.append(names[0])
            record.append(names[1])
            record.append(strdate)
            record.append(datetime.datetime.now().strftime('%H%M%S'))
            record.append(score[0] + "-" + score[1])

            rec_list.append(record)
            rec_tuple.append(tuple(record))

            i += 1

        return rec_list, rec_tuple

    @staticmethod
    def summarized(text, rowcount):
        try:
            sentences, debug_info = summarize(
                text, sent_limit=rowcount, continuous=True, debug=True
            )
        except:
            sentences = "sammarized error"

        return sentences


def main():
    RA = RecordAccumulation()
    today = datetime.date(2018, 4, 18)
    test = RA.get_jp_s_score(today)


if __name__ == '__main__':
    main()

