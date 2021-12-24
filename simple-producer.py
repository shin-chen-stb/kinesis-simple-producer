# coding: utf-8

import json
from boto import kinesis
import time
import sys

# id=sys.argv[0]
# key=sys.argv[1]
id = 'AKIARLZ6R52JFKELZTQN'
key = 'LI5sQlJK/F+VGA+zyZJ4QAgA7vZSzviWDpLpyph4'
stream_name = 'ddp-job-store-cdc-pipeline-stream'
auth = {"aws_access_key_id": id, "aws_secret_access_key": key}
connection = kinesis.connect_to_region('ap-northeast-1', **auth)


def lambda_handler(event):
    print(event)
    data = json.dumps(event)
    connection.put_record(stream_name=stream_name,
                          data=data, partition_key='stest')


#data = {"id": '15', "name": "chen", "age": '12', "sex": "maie"}
data = {
  "job_id": "be05783339fdf011dad43a9541e3e2a5ddb444d238d28d35a7e55faa7b97b501",
  "update_date": "2021-09-21T03:28:43.303Z",
  "job_version_date": "2021-12-23T03:00:11.059Z",
  "site_code": "mynavi-baito",
  "site_name": "マイナビバイト",
  "def_code": "mynavi-baito_imp",
  "job_url": "https://baito.mynavi.jp/cl-002216704532/job-40731006/",
  "job_title": "シール貼り 袋詰め 登録制 好きな日・好きな時間に仕事IN 全額日払い×現金手渡し 派遣社員",
  "index_type": "xml",
  "job_type": [
    "TEMP"
  ],
  "job_type_raw": "派遣社員",
  "job_content": "【シール貼り/袋詰め】\n*登録制*好きな日・好きな時間に仕事IN♪全額日払い×現金手渡し★\n\nteikeiworksTOKYO 藤沢支店\n\n■ 給与\n時給1150円~\n\n■ シフト\n週1日以上、1日4時間以上\n\n■ アクセス\n東海道本線(東京−熱海)戸塚駅\n\n■ 時間帯\n早朝、朝、昼、夕方、夜、夜勤、残業月10時間以下\n\n■ 勤務地\n横浜市 戸塚区\n\n● お仕事情報\n\n■ 仕事内容\n■■ 激短1日も！週1~も！自由自在でOK ■■\n\n【シール貼り/袋詰め】\n今流行りの、人気商品・あのお馴染みのものetc..\nペタっと商品にシールを貼ったり、袋に入れたり♪\n＼やることはコレだけ★／\n\n▼例えば、こんなお仕事をお任せ♪*\n□ コンビニ商品のラベル貼り\n□ お菓子のラベル貼り\n□ 通販商品のシール貼り\n□ キャラクターグッズへのシール貼り\n□ おもちゃやぬいぐるみへのシール貼り\n□ チョコレートの袋詰め\n□ お菓子やおもちゃの袋詰め\n\n※登録制のため、ご応募のタイミングによりご紹介可能な案件が異なります。\n\n＜＜ここがポイント＞＞\n体を動かす仕事\n\n■ 給与\n時給1150円~\n\n◆昇給あり\n\n※案件・時間帯によって変動あり\n※残業手当あり\n\n★初勤務手当を支給中!!★\n初勤務日に【1日1000円】を支給します♪\n\n◆支払い方法：日払い\n\n◆交通費：なし\n\n※案件によって金額変動あり\n\n■ 所在地\n神奈川県 藤沢市 藤沢388 富士ビル新館2F\n\n■ アクセス\n◆東海道本線(東京−熱海)戸塚駅\n◆横須賀線戸塚駅\n◆横浜市営地下鉄ブルーライン（関内−湘南台）戸塚駅\n◆湘南新宿ライン(高崎線−東海道本線)戸塚駅\n◆湘南新宿ライン(東北本線−横須賀線)戸塚駅\n\n■ 勤務期間\n短期（1日）／短期（1週間以内）／短期（1ヶ月以内）／短期（3ヶ月以内）／長期（3ヶ月以上）／春/夏/冬休み期間限定\n※登録制のため、好きな日に好きなだけ働けます！\n\n■ シフト・勤務時間\n週1日以上、1日4時間以上\n24時間・365日好きなときにシフトin♪\n\n■ 勤務できる曜日\n月、火、水、木、金、土、日\n【休日・休暇】シフト自由！\n\n■ 応募資格\n★゜+. みなさんWelcome ゜+.★\n学生さん、主婦(夫)さん、フリーターさん、\nWワーカーさん、中高年さん etc… \n\n《必須》\n◆携帯電話をお持ちの方\n※応募後、お仕事紹介のメールを、\n 携帯電話で受け取ることが必須のため。\n\n※過度な染髪・ヒゲはご遠慮ください\n未経験者歓迎、経験者優遇、フリーター歓迎、Wワーク歓迎、大学生歓迎、二部学生歓迎、学歴不問、友達と応募歓迎、主婦（夫）歓迎、履歴書不要\n\n■ 待遇・福利厚生\n★履歴書不要→即登録→即勤務→お給料GET\n★日払い&週払いOK (規定有)\n★お仕事により、制服貸与有\n★昇給あり\n★勤務地によりバス代一部支給\n★勤務地により手当有\n★勤務地により無料送迎有\n★社員登用制度有\n★友達紹介制度有 (最大6万円まで支給!!)※規定有\n★フォーク資格取得制度あり (日当保障)\n社会保険完備、制服貸与、送迎制度あり\n\n● 応募情報\n\n■ 会社・店舗名\nteikeiworksTOKYO 藤沢支店\n\n■ 職種名\nシール貼り/袋詰め（*登録制*好きな日・好きな時間に仕事IN♪全額日払い×現金手渡し★）\n\n■ 事業内容\n労働者派遣法に基づく一般労働者派遣事業\nアウトソーシング全般\n物品の仕分け、梱包及び発送及び配送業務の請負\n有料職業紹介業\n\n一般労働者派遣事業許可No：派13-305133\n有料職業紹介事業許可No：13-ユ-305513",
  "salary": {
    "unit": "HOR",
    "max": 1150,
    "min": 1150
  },
  "salary_raw": "時給1150円~",
  "geo_raw_data": "{\"tookTime\":4,\"offset\":0,\"count\":1,\"total\":0,\"locations\":[{\"point\":{\"type\":\"Feature\",\"properties\":{},\"geometry\":{\"type\":\"Point\",\"coordinates\":[139.6273040771,35.4851531982]}},\"bound\":{\"type\":\"Feature\",\"properties\":{},\"geometry\":{\"type\":\"Bound\",\"coordinates\":[[139.6273040771,35.4851531982]]}},\"address\":{\"addressLevel\":3,\"originalText\":\"神奈川県横浜市東海道本線(東京-熱海)戸塚駅\",\"fullName\":\"神奈川県横浜市\",\"fullKana\":None,\"postCode\":None,\"governmentCode\":\"14100\",\"prefecture\":\"神奈川県\",\"county\":None,\"city\":\"横浜市\",\"quarter\":None,\"neighbourhood\":None,\"blockNumber\":None,\"houseNumber\":None,\"building\":None,\"unknown\":\"東海道本線戸塚駅\"}}]}",
  "company_name": "teikeiworksTOKYO 藤沢支店",
  "work_location": {
    "location": "神奈川県 横浜市 東海道本線(東京−熱海)戸塚駅",
    "address": [
      {
        "original_text": "神奈川県横浜市東海道本線(東京-熱海)戸塚駅",
        "city": "横浜市",
        "prefecture": "神奈川県",
        "full_name": "神奈川県横浜市",
        "government_code": "14100",
        "address_level": 3,
        "quarter": None,
        "unknown": "東海道本線戸塚駅",
        "point": {
          "lon": 35.4851531982,
          "lat": 139.6273040771
        }
      }
    ]
  },
  "ad_category": [
    "軽作業"
  ],
  "open_date": "2021-08-25T08:31:12.422Z",
  "learn_raw_data": "{\"documentId\":\"be05783339fdf011dad43a9541e3e2a5ddb444d238d28d35a7e55faa7b97b501\",\"learn_update_dt\":\"2021-12-23T03:00:11.053739\",\"tfidfSum_f\":0.0,\"jobLabelId_s\":[\"lightworksetc\"],\"jobLabel_s\":[\"picking\"],\"jobCategory_t\":[\"シール貼り\",\"接客軽作業\",\"工場内軽作業\",\"除染\",\"軽作業\",\"軽作業パートフルタイム\",\"物流\"],\"signalId_s\":[\"noneEducation\",\"qualificationSupport\",\"student\",\"shiftFree\",\"housewife\",\"doubleWork\",\"withRecruitmentChance\",\"treatExperienced\",\"resumeUnnecessary\",\"beginnerOk\",\"withUniform\",\"handySalaryOk\",\"withMovingBody\",\"withSocialInsurance\",\"withRaiseSalary\",\"freeter\"],\"signalLabel_s\":[\"noneEducation\",\"qualificationSupport\",\"student\",\"shiftFree\",\"housewife\",\"doubleWork\",\"withRecruitmentChance\",\"treatExperienced\",\"resumeUnnecessary\",\"beginnerOk\",\"withUniform\",\"handySalaryOk\",\"withMovingBody\",\"withSocialInsurance\",\"withRaiseSalary\",\"freeter\"],\"ml\":{\"20210707\":{\"job_category\":4,\"job_vector_1\":None,\"predict_ctr\":None,\"job_richness_score\":None},\"20211013\":{\"job_category\":4,\"job_vector_1\":None,\"predict_ctr\":0.25673869277648115,\"job_richness_score\":None},\"20220126\":{\"job_category\":4,\"job_vector_1\":None,\"predict_ctr\":0.25673869277648115,\"job_richness_score\":0.8}},\"queryRank_s\":[],\"queryRank1_s\":[\"1日\",\"town\",\"お菓子\",\"アニメイベント\",\"アルバイト・パート仕分け\",\"アルバイト・パート手渡し\",\"アルバイト・パート現金手渡し\",\"エントリー派遣社員\",\"サンレディース\",\"モクモク\",\"ラベル\",\"値札\",\"単発\",\"封入\",\"手渡し\",\"手渡し日払い\",\"日雇い\",\"現金手渡し\",\"短期\",\"箱詰め\",\"袋詰め\",\"食品\",\"アルバイト・パートフルキャスト\",\"アルバイト・パート単発\",\"シール\",\"フルキャスト\",\"交通量調査\",\"果物\",\"短期高校生\",\"アルバイト・パート簡単\",\"土日のみwワーク\",\"登録\",\"短期アルバイト\",\"ダム\",\"ピッキング派遣社員\",\"フルキャスト派遣社員\",\"シール貼り\",\"仕訳\",\"短期日払い\",\"化粧品派遣社員\"],\"queryRank2_s\":[],\"yahooCompanyId\":-1,\"companyNameLevel_i\":2}",
  "yahoo_company_id": -1,
  "query_rank1": [
    "1日",
    "town",
    "お菓子",
    "アニメイベント",
    "アルバイト・パート仕分け",
    "アルバイト・パート手渡し",
    "アルバイト・パート現金手渡し",
    "エントリー派遣社員",
    "サンレディース",
    "モクモク",
    "ラベル",
    "値札",
    "単発",
    "封入",
    "手渡し",
    "手渡し日払い",
    "日雇い",
    "現金手渡し",
    "短期",
    "箱詰め",
    "袋詰め",
    "食品",
    "アルバイト・パートフルキャスト",
    "アルバイト・パート単発",
    "シール",
    "フルキャスト",
    "交通量調査",
    "果物",
    "短期高校生",
    "アルバイト・パート簡単",
    "土日のみwワーク",
    "登録",
    "短期アルバイト",
    "ダム",
    "ピッキング派遣社員",
    "フルキャスト派遣社員",
    "シール貼り",
    "仕訳",
    "短期日払い",
    "化粧品派遣社員"
  ],
  "job_category": [
    "シール貼り",
    "接客軽作業",
    "工場内軽作業",
    "除染",
    "軽作業",
    "軽作業パートフルタイム",
    "物流"
  ],
  "job_label": [
    "picking"
  ],
  "signal_label": [
    "noneEducation",
    "qualificationSupport",
    "student",
    "shiftFree",
    "housewife",
    "doubleWork",
    "withRecruitmentChance",
    "treatExperienced",
    "resumeUnnecessary",
    "beginnerOk",
    "withUniform",
    "handySalaryOk",
    "withMovingBody",
    "withSocialInsurance",
    "withRaiseSalary",
    "freeter"
  ],
  "duplicate_hash": {
    "perfect_match_in_cp": "e79f62aac2f634a7f1a43cdaac03e48b6e86eaec90e58f95d72aaaf757390bba",
    "cassette_open_all_in_cp": "31251aea19e16ecf1b807ce407256fc93f959a437948acc0ce483ff401e4846d"
  },
  "job_history_version": "V1.2.0",
  "action_type": "insert",
  "locked_reasons": [
    {
      "lock_type": "DUPLICATE",
      "reason": "労働時間が記述されていないため"
    }
  ]
}
#data = json.dumps(data_str, ensure_ascii=False).encode('utf8')
for i in range(10):
    lambda_handler(data)
