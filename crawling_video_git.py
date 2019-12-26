#!/usr/bin/python

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.tools import argparser
import pandas as pd
import numpy as np
import datetime
import mysql.connector
import argparse
import sys

class MyArgs:
    def __init__(self, q, videoCategoryId, maxResults):
        self.q = q
        self.videoCategoryId = videoCategoryId
        self.maxResults = maxResults

DEVELOPER_KEY = # APIKey #
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

def youtube_search(args: MyArgs):
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)

    # HTTP Requestを受け取る箱を定義
    videos = []
    category = []
    channel = []
    comment = []
    commentThreads = []
####Search####
    # videoの詳細情報を抽出するときの事前設定
    search_response = youtube.search().list(
        q = args.q,
        part="snippet",
        type="video",
        videoCategoryId = args.videoCategoryId,
        maxResults=args.maxResults
        ).execute()
    
    ## videoの詳細情報を抽出
    for search_result in search_response.get("items", []):
        if search_result["id"]["kind"] == "youtube#video":
            # title = search_result["snippet"]["title"] # タイトル
            video_id = search_result["id"]["videoId"] # 動画ID
            # description = search_result["snippet"]["description"] # 動画の概要
            # thumbnail = search_result["snippet"]["thumbnails"]["default"]["url"] # サムネ
            upload_date = search_result["snippet"]["publishedAt"] # 動画のアップロード日時
        # Dataframeのアウトプットを考慮して辞書型で情報を取得
        videos.append({
            # 'タイトル': title,
            '動画ID' : video_id,
            # '動画の概要' : description,
            # 'サムネ' : thumbnail,
            '動画のアップロード日時' : upload_date
            })
        # listをDataframe化する
        df_videos = pd.DataFrame(
            videos,
            columns = [
                # 'タイトル',
                '動画ID',
                # '動画の概要',
                # 'サムネ',
                '動画のアップロード日時'
                ]
                )
        print('########確認_videos#########', df_videos)

        ## videoのカテゴリ情報を抽出
        # categoryを抽出するときの事前設定
        search_response_video = youtube.videos().list(
            part="id, snippet, statistics",
            id = search_result['id']['videoId'],
            maxResults=args.maxResults
            ).execute()
        
        for search_result_video in search_response_video.get("items", []):
            if search_result_video["kind"] == "youtube#video":
                # title = search_result_video["snippet"]["title"] # タイトル
                # video_id = search_result_video["id"] # 動画ID
                # describe = search_result_video["snippet"]["description"] # 動画の概要
                # video_category = search_result_video["snippet"]["categoryId"] # 動画のカテゴリ
                # video_viewCount = search_result_video["statistics"]["viewCount"] # 動画の再生回数
                # video_commentCount = search_result_video["statistics"]["commentCount"] # 動画に対するコメント数★2019/8/20にKeyerror回避用コメントアウト
                # video_likeCount = search_result_video["statistics"]["likeCount"] # 高い評価の数
                # print('-------------いいね数-------------', video_likeCount)
                # video_dislikeCount = search_result_video["statistics"]["dislikeCount"] # 低い評価の数
                # video_channelId = search_result_video["snippet"]["channelId"] # 動画に紐づくChannnelId
                try:
                    title = search_result_video["snippet"]["title"] # タイトル
                    video_id = search_result_video["id"] # 動画ID
                    describe = search_result_video["snippet"]["description"] # 動画の概要
                    video_category = search_result_video["snippet"]["categoryId"] # 動画のカテゴリ
                    video_viewCount = search_result_video["statistics"]["viewCount"] # 動画の再生回数
                    video_commentCount = search_result_video["statistics"]["commentCount"] # 動画に対するコメント数★2019/8/20にKeyerror回避用コメントアウト
                    video_likeCount = search_result_video["statistics"]["likeCount"] # 高い評価の数
                    print('-------------いいね数-------------', video_likeCount)
                    video_dislikeCount = search_result_video["statistics"]["dislikeCount"] # 低い評価の数
                    video_channelId = search_result_video["snippet"]["channelId"] # 動画に紐づくChannnelId
                except KeyError:
                    continue

            # Dataframeのアウトプットを考慮して辞書型で情報を取得
            category.append({
                'タイトル' : title,
                '動画ID' : video_id,
                '動画の概要' : describe,
                '動画のカテゴリ' : video_category,
                '動画の再生回数' : video_viewCount,
                '動画に対するコメント数' : video_commentCount, # ★2019/8/20
                '高い評価の数' : video_likeCount,
                '低い評価の数' : video_dislikeCount,
                'チャンネルID' : video_channelId
                })
        # listをDataframe化する
        df_category = pd.DataFrame(
            category,
            columns = [
                'タイトル',
                '動画ID',
                '動画の概要',
                '動画のカテゴリ',
                '動画の再生回数',
                '動画に対するコメント数', # ★2019/8/20
                '高い評価の数',
                '低い評価の数',
                'チャンネルID'
                ]
            )
        print('########確認_category#########', df_category)

        # channel情報を取得するための事前設定
        search_response_channel = youtube.channels().list(
            part="id, statistics, status",
            id = search_result_video["snippet"]["channelId"],
            maxResults=args.maxResults
            ).execute()        

        ## Extract commentThread of videos
        for search_result_channel in search_response_channel.get('items', []):
            if search_result_channel['kind'] == 'youtube#channel':
                c_channelId = search_result_video["snippet"]["channelId"] # チャンネルID
                c_subscriberCount = search_result_channel['statistics']['subscriberCount'] # チャンネル登録者数
                c_viewCount = search_result_channel['statistics']['viewCount'] # チャンネルの再生回数
                c_commentCount = search_result_channel['statistics']['commentCount'] # チャンネルのコメント数
                c_videoCount = search_result_channel['statistics']['videoCount'] # チャンネルにアップロードされた動画数
                # c_hiddenSubscriberCount = search_result_channel['statistics']['hiddenSubscriberCount'] # チャンネル登録者数を公開表示するかどうかの指定
                # c_privacyStatus = search_result_channel['status']['privacyStatus'] # チャンネルのプライバシーステータス

            # Dataframeのアウトプットを考慮して辞書型で情報を取得
            channel.append({
                'チャンネルID' : c_channelId,
                'チャンネル登録者数' : c_subscriberCount,
                'チャンネルの再生回数' : c_viewCount,
                'チャンネルのコメント数' : c_commentCount,
                'チャンネルにアップロードされた動画数' : c_videoCount,
                # 'チャンネル登録者数を公開表示するかどうかの指定' : c_hiddenSubscriberCount,
                # 'チャンネルのプライバシーステータス' : c_privacyStatus
            })
                # listをDataframe化する
        df_channel = pd.DataFrame(
            channel,
            columns = [
                'チャンネルID',
                'チャンネル登録者数',
                'チャンネル再生回数',
                'チャンネルのコメント数',
                'チャンネルにアップロードされた動画数',
                # 'チャンネル登録者数を公開表示するかどうかの指定',
                # 'チャンネルのプライバシーステータス'
                ]
            )
        print('########確認_channel#########', df_channel)


        # commentを抽出するときの事前設定
        search_response_commentThreads = youtube.commentThreads().list(
            part = 'id, snippet, replies',
            videoId = search_result['id']['videoId'],
            # order = 'relevance', # グッド数とコメント数が多い順に取得
            maxResults = args.maxResults
            ).execute()

        ## Extract commentThread of videos
        for search_result_commentThreads in search_response_commentThreads.get('items', []):
            if search_result_commentThreads['kind'] == 'youtube#commentThread':
                video_id = search_result_commentThreads['snippet']['videoId']
                topLevelComment = search_result_commentThreads['snippet']['topLevelComment']
                comment_id = topLevelComment['id']
                textDisplay = topLevelComment['snippet']['textDisplay']
                likeCount = topLevelComment['snippet']['likeCount']



            commentThreads.append({
                '動画ID' : video_id,
                'コメントID' : comment_id,
                'コメント' : textDisplay,
                'コメントのいいね数' : likeCount,
                'トップのコメント' : topLevelComment, # この中に辞書として格納されている'textDisplay', 'likeCOunt', 'publishedAt'を抽出する★TODO(2019/6/29)
                # 'リプライされている数' : totalReplyCount
                # 'Publicの可否' : isPublic
                })
        # listをDataframe化する
        df_commentThreads = pd.DataFrame(
            commentThreads,
            columns = [
                '動画ID',
                'コメントID',
                'コメント',
                'コメントのいいね数',
                'コメントのアップ日時',
                'トップのコメント',
                # 'リプライされている数'
                # 'Publicの可否'
                ]
            )
        print('########確認_commentThreads', df_commentThreads)

    try:
        df_merge_semi = (
            pd.merge(df_videos, df_category, on = '動画ID', how = 'inner')
            )

        df_merge_semi_final = (
            pd.merge(df_merge_semi, df_channel, on = 'チャンネルID', how = 'inner')
            )

        df_merge = (
            pd.merge(df_merge_semi_final, df_commentThreads, on = '動画ID', how = 'inner')
            )
        # # 動画のアップロード日時を比較できるようstringからdatetime型に変更
        # df_merge['動画のアップロード日時'] = pd.to_datetime(df_merge['動画のアップロード日時'])

        # 動画の抽出ロジック
        df_merge['動画の再生回数'] = df_merge['動画の再生回数'].fillna(0).astype(np.int64)
        df_merge['高い評価の数'] = df_merge['高い評価の数'].fillna(0).astype(np.int64)
        df_merge['低い評価の数'] = df_merge['低い評価の数'].fillna(0).astype(np.int64)
        df_merge['動画に対するコメント数'] = df_merge['動画に対するコメント数'].fillna(0).astype(np.int64) # ★2019/8/20
        df_merge['チャンネルにアップロードされた動画数'] = df_merge['チャンネルにアップロードされた動画数'].fillna(0).astype(np.int64)
        df_merge['チャンネル登録者数'] = df_merge['チャンネル登録者数'].fillna(0).astype(np.int64)
        df_merge['チャンネルにアップロードされた動画数'] = df_merge['チャンネルにアップロードされた動画数'].fillna(0).astype(np.int64)


        # 抽出ロジックからscoreの作成
        df_merge_score = df_merge.assign(
            score = (
                (df_merge['高い評価の数'] * 1)
                + (df_merge['チャンネル登録者数'] * 0.5)
                + (df_merge['動画の再生回数'] * 1)
                + (df_merge['動画に対するコメント数'] * 0.5) # ★2019/8/20
                + (df_merge['チャンネルにアップロードされた動画数'] * 0.3)
            )
        )

        # youtubeで用語を検索した時の検索結果順をgscoreとして順番を残す
        df_merge_gscore = df_merge_score.assign(
            gscore = (df_merge_score.index + 1)
        )



        # 動画のアップロード日時からscoreというカラムでランク付け
        df_merge_rank = df_merge_gscore.assign(
            rank = df_merge_score['score'].rank(
                axis = 0,
                ascending = False,
                method = 'min',
                na_option = 'keep'
                )
            )
        
        # 低い評価が極端に多い動画は抽出対象外とする
        df_merge_flg = df_merge_rank.assign(
            flg = (df_merge_score['高い評価の数']) / (df_merge_score['低い評価の数'])
        )
        df_merge_rate = df_merge_flg.where(df_merge_flg['flg'] > 10)


        # 動画のスクレイピングしたタイミングをupdate_dtというカラムで記録しておく
        df_merge_update = df_merge_rate.assign(
            update_dt = datetime.date.today()
            )

        # 動画IDからurlのカラムを追加する
        df_merge_url = df_merge_update.assign(
            url = 'https://www.youtube.com/watch?v=' + df_merge_update['動画ID']
        )

        df_target_semi = df_merge_url.loc[:,
        [
            '動画のカテゴリ',
            '動画の概要',
            'タイトル',
            'url',
            '高い評価の数',
            '低い評価の数',
            'チャンネル登録者数',
            '動画に対するコメント数', # ★2019/8/20
            'コメントID',
            'コメント',
            'gscore',
            'score',
            'update_dt'
            ]
            ]
        df_target_last = df_target_semi.drop_duplicates().dropna(axis = 0, how = 'any').reset_index(drop = True)
        print('アウトプットの確認', df_target_last)




        # MySQLに書き出し
        conn = mysql.connector.connect(
            host = 'database-1.clya2mwzys8b.ap-northeast-1.rds.amazonaws.com',
            user = 'admin',
            password = 'digitalhack',
            database = 'whetdict',
            auth_plugin = 'mysql_native_password'
            )
        cur = conn.cursor(buffered=True)
        # VALUESの後に入れたい変数を設定する。★TODO（2019/6/30）
        # main1 = df_target_last.set_index
        # main1 = df_target_last.index.map(np.str)
        main1 = '2'
        main2 = df_target_last['動画のカテゴリ'].fillna(0).astype(np.str)
        print(main2)
        main3 = df_target_last['url'].fillna(0).astype(np.str)
        print(main3)
        main4 = df_target_last['動画の概要'].fillna(0).astype(np.str)
        print(main4)
        main5 = df_target_last['タイトル'].fillna(0).astype(np.str)
        print(main5)
        main6 = df_target_last['高い評価の数'].fillna(0).astype(np.str)
        print(main6)
        main7 = df_target_last['低い評価の数'].fillna(0).astype(np.str)
        print(main7)
        main8 = df_target_last['チャンネル登録者数'].fillna(0).astype(np.str)
        print(main8)
        main9 = df_target_last['動画に対するコメント数'].fillna(0).astype(np.str)
        print(main9)
        main10 = df_target_last['コメントID'].fillna(0).astype(np.str)
        print(main10)
        main11 = df_target_last['コメント'].fillna(0).astype(np.str)
        print(main11)
        main12 = df_target_last['gscore'].astype(np.str)
        print(main12)
        main13 = df_target_last['score'].fillna(0).astype(np.str)
        print(main13)
        main14 = df_target_last['update_dt'].astype(np.str)
        print(main14)

        
        for i in range(0, len(df_target_last) - 1, 1):
            sent = "INSERT INTO wp_video(`keyword`,`category`,`url`,`description`,`title`,`likes`,`dislikes`,`subscribers`,`comments`,`commentid`,`comment`,`gscore`,`score`,`update_dt`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            # sent = "INSERT INTO wp_video(`keyword`,`category`,`url`,`description`,`title`,`likes`,`dislikes`,`subscribers`,`comments`,`commentid`,`comment`,`gscore`,`score`,`update_dt`) VALUES ('"+main1+"','"+main2[i]+"','"+main3[i]+"','"+main4[i]+"','"+main5[i]+"','"+main6[i]+"','"+main7[i]+"','"+main8[i]+"','"+main9[i]+"','"+main10[i]+"','"+main11[i]+"','"+main12[i]+"','"+main13[i]+"','"+main14[i]+"')"
            print('SQLのコード', sent)
            print('SQLのレコード', i)
            cur.execute(sent, (main1, main2[i], main3[i], main4[i], main5[i], main6[i], main7[i], main8[i], main9[i], main10[i], main11[i], main12[i], main13[i], main14[i]))
            conn.commit()
    except UnboundLocalError:
        break

# MySQLからKeywordを抽出する
conn = mysql.connector.connect(
    host = 'database-1.clya2mwzys8b.ap-northeast-1.rds.amazonaws.com',
    user = 'admin',
    password = 'digitalhack',
    database = 'whetdict',
    auth_plugin = 'mysql_native_password'
    )
cur = conn.cursor(buffered=True)
# whetdict.wp_trendから最新の検索用のkeywordを抽出する
sql = "SELECT keyword FROM wp_trend WHERE regdate = (SELECT max(regdate) FROM wp_trend)"
cur.execute(sql)
result = cur.fetchall()
print('キーワードの型:', type(result))
print('Tupleか確認', type(result[0]))
conn.commit()
print('wp_trendから抽出しているTupleの長さを確認', len(result))

if __name__ == "__main__":
    
    argparser.add_argument("--q", nargs = '+', help="Search term")
    argparser.add_argument("--videoCategoryId", help="Search category", default='19')
    argparser.add_argument("--maxResults", help="Max results", default=3)
    args = argparser.parse_args()

    for  i in range(60, len(result) - 1, 1):
        print('ワード検索', ''.join(result[i]))
        print('ワードの番号', i)
        try:
            youtube_search_args = MyArgs(
                q = ''.join(result[i]),
                videoCategoryId = args.videoCategoryId,
                maxResults = args.maxResults
            )
            youtube_search(youtube_search_args)
            continue
        except HttpError as e:
            print("An HTTP error %d occurred:\n%s" % (e.resp.status, e.content))