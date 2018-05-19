# Copyright 2018 Ct,innovation. All Rights Reserved.
#
import datetime
import json
from flask import Flask
from flask import request
from flask import make_response
import sportslive
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account

app = Flask(__name__)
SL = sportslive.SportsLive()


@app.route('/webhook', methods=['POST'])
def webhook():
    req = request.get_json(silent=True, force=True)
    res = processRequest(req)
    res = json.dumps(res, indent=4)
    r = make_response(res)
    r.headers['Content-Type'] = 'application/json'
    return r


@app.route('/news-loader', methods=['GET'])
def newsloader():
    json_dict = {}
    query = request.args.get('query')
    querylist = query.split('_')
    query = querylist[0]
    rowcount = int(querylist[1])
    day = querylist[2]
    
    try:    
        if query is None:
            return 'No provided.', 400
        if rowcount is None:
            rowcount = 2
        if day is None:
            day = datetime.date.today()
            tdatetime = day.strftime('%Y-%m-%d')
        else:
            tdatetime = day
    except:
        json_dict.update({'error':
                         {
                         'text':"format miss"
                         }}
                         )
        encode_json_data = json.dumps(json_dict)
        return encode_json_data 
    
    try:
        result = SL.news_loader(query, rowcount, tdatetime)
        result = json.dumps(result, indent=4)
    except NameError as e:
        json_dict.update({'error':
                         {
                         'args':e.args,
                         'date':tdatetime    
                         }}
                         )
        encode_json_data = json.dumps(json_dict)
        return encode_json_data 
        
    except:
        json_dict.update({'error':
                         {
                         'date':"aaaaa"
                         }}
                         )
        encode_json_data = json.dumps(json_dict)
        return encode_json_data 
    
    if result is None:
        return 'not found : %s' % query, 400
    return result, 200


@app.route('/debug/news-loader', methods=['GET'])
def newsloader_debug():
    """Given an query, return that news debug mode."""
    query = request.args.get('query')
    querylist = query.split('_')
    query = querylist[0]
    rowcount = int(querylist[1])
    day = querylist[2]
    json_dict = {}

    if query is None:
        return 'No provided.', 400
    if rowcount is None:
        rowcount = 2
    if day is None:
        day = datetime.date.today()
        tdatetime = day.strftime('%Y-%m-%d')
    else:
        tdatetime = day
    result = SL.news_loader(query, rowcount, tdatetime, debug=True)
    result = json.dumps(result, indent=4)
    if result is None:
        return 'not found : %s' % query, 400
    return result, 200


@app.route('/player-loader', methods=['GET'])
def playerloader():
    """Given an query, return that news."""
    query = request.args.get('query')
    querylist = query.split('_')
    query = querylist[0]
    day = querylist[1]
    json_dict = {}
    
    if query is None:
        return 'No provided.', 400
    if day is None:
        day = datetime.date.today()
        tdatetime = day.strftime('%Y-%m-%d')
    else:
        tdatetime = day
        
    result = SL.player_loader(query, tdatetime)
    result = json.dumps(result, indent=4)
    if result is None:
        return 'not found : %s' % query, 400
    return result, 200


@app.route('/debug/player-loader', methods=['GET'])
def playerloader_debug():
    """Given an query, return that news debug mode."""
    query = request.args.get('query')
    querylist = query.split('_')
    query = querylist[0]
    day = querylist[1]
    json_dict = {}

    if query is None:
        return 'No provided.', 400
    if day is None:
        day = datetime.date.today()
        day = day.strftime('%Y%m%d')
        tdatetime = day.strftime('%Y-%m-%d')
    else:
        tdatetime = day        
    result = SL.player_loader(query, tdatetime, debug=True)
    result = json.dumps(result, indent=4)
    if result is None:
        return 'not found : %s' % query, 400
    return result, 200


@app.route('/news-reader', methods=['GET'])
def newsreader():
    """Given an query, return that news."""
    query = request.args.get('query')
    if query is None:
        return 'No provided.', 400
    result = SL.news_check(query)
    if result is None:
        return 'not found : %s' % query, 400
    return result, 200


@app.route('/debug/news-reader', methods=['GET'])
def newsreader_debug():
    """Given an query, return that news debug mode."""
    query = request.args.get('query')
    if query is None:
        return 'No provided.', 400
    result = SL.news_check(query, debug=True)
    if result is None:
        return 'not found : %s' % query, 400
    return result, 200


@app.route('/summarize', methods=['GET'])
def summarize():
    """Given an query, return that news."""
    query = request.args.get('query')
    querylist = query.split('_')
    query = querylist[0]
    rowcount = int(querylist[1])
    
    if query is None:
        return 'No provided.', 400
    result = SL.summarized(query, rowcount)
    if result is None:
        return 'not found : %s' % query, 400
    return result, 200


@app.route('/add-record', methods=['GET'])
def add_record():
    json_dict = {}
    ra = sportslive.RecordAccumulation()
    """Given an date, records add to table ."""

    day = request.args.get('query').split('-')
    if day is None:
        day = datetime.date.today()
    day = datetime.date(int(day[0]), int(day[1]), int(day[2]))
    tdatetime = day.strftime('%Y%m%d')

    # news取得フェーズ
    news_record, news_record_tuple = ra.news_check(day)
    if len(news_record_tuple) != 0:
        # ra.save_csv(news_record, "news_record.csv")
        result = load_data("newsrecord${}".format(tdatetime),
                           news_record_tuple)

    # player成績取得フェーズ（野球）
    try:
        player_record, player_record_tuple = ra.get_jp_bplayer_record(day)
        # ra.save_csv(player_record, "player_record.csv")
    except:
        json_dict.update({'error':
                         {
                         'title':'player get error'
                         }}
                         )
        encode_json_data = json.dumps(json_dict)
        return encode_json_data 
    
    try:
        result = load_data("bplayerrecord${}".format(tdatetime),
                           player_record_tuple)
    except:
        json_dict.update({'error':
                         {
                         'text':"load error player",
                         'list':player_record_tuple
                         }}
                         )
        encode_json_data = json.dumps(json_dict)
        return encode_json_data

    # score取得フェーズ(野球)
    try:
        score_record, score_record_tuple = ra.get_jp_b_score(day)
    except:
        json_dict.update({'error':
            {
                'title': 'score get error'
            }}
        )
        encode_json_data = json.dumps(json_dict)
        return encode_json_data

    try:
        result = load_data("scorerecord${}".format(tdatetime),
                           score_record_tuple)
    except:
        json_dict.update({'error':
            {
                'text': "load error player",
                'list': player_record_tuple
            }}
        )
        encode_json_data = json.dumps(json_dict)
        return encode_json_data

    # score取得フェーズ(サッカー)
    try:
        score_record, score_record_tuple = ra.get_jp_s_score(day)
    except:
        json_dict.update({'error':
            {
                'title': 'score get error'
            }}
        )
        encode_json_data = json.dumps(json_dict)
        return encode_json_data

    try:
        result = load_data("scorerecord${}".format(tdatetime),
                           score_record_tuple)
    except:
        json_dict.update({'error':
            {
                'text': "load error player",
                'list': player_record_tuple
            }}
        )
        encode_json_data = json.dumps(json_dict)
        return encode_json_data

    json_dict.update({'completed':
                         {
                         'text':player_record_tuple
                         }}
                         )
    encode_json_data = json.dumps(json_dict)
    return encode_json_data, 200


def load_data(table_id, source):
    json_key = 'sports-agent-9947da5a2148.json'
       
    try:
        bigquery_client = bigquery.Client.from_service_account_json(json_key, project='deep-equator-204407')
        # bigquery_client = bigquery.Client(project='deep-equator-204407')
        # bigquery_client = bigquery.Client()
        dataset_ref = bigquery_client.dataset("sportsagent")
    except:
        raise NameError('client dont getting')
        
    try:
        table_ref = dataset_ref.table(table_id)
        table = bigquery_client.get_table(table_ref)
        errors = bigquery_client.insert_rows(table, source) 
    except:
        raise NameError(type(source))
    
    return errors


def processRequest(req):
    actiontype = req.get("result").get("action")
    if actiontype == "reply_to_player_record":
        SL.execute_sql(req.get("result").get("action"), ["name", "record"], )
    elif actiontype == "reply_to_news":
        SL.execute_sql()
    elif actiontype == "reply_to_soccer_score":
        pass
    elif actiontype == "reply_to_baseball_score":
        pass
    else:
        return {}

    return res

    
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)

