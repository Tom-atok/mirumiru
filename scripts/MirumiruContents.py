import pandas as pd
import re

class MirumiruContents:
  def __init__(self,url):
    #urlには参照にするgoogle spread sheetのurlがわたされる 
    self.url = url

  def authentication(self):
    """このプログラムがgoogle driveにアクセスできるように認証する．認証後，gcを返す"""
    auth.authenticate_user()
    creds, _ = default()
    gc = gspread.authorize(creds)
    return gc
  
  def gs_to_df(self,gc):
    """スプレッドシートをデータフレームに変換する"""
    #ワークシート単位でないと取り出せないので，ワークシートを取り出して，データフレームに変換する
    ss = gc.open_by_url(self.url)
    ws = ss.get_worksheet(0)
    df =  pd.DataFrame(ws.get_all_values())
    #0行目をカラム名に変更する
    df.columns = list(df.loc[0,:])
    df.drop(0,inplace=True)
    df.reset_index(inplace=True)
    df.drop('index',axis=1, inplace=True)

    return df

  #######
  #submit用のデータ整形メソッド
  #######
  def rename_columns_submit(self,df):
    """カラム名を英語に変換する，フォームの質問事項を変えたら変更する"""
    check_list_submit = ['タイムスタンプ','ニックネーム','魅力的に感じた本郷キャンパスの写真を投稿してください．',
                         '写真を撮った場所を教えてください(わかる範囲で大丈夫です)\n','魅力的に感じた点を教えてください',
                         '非公開，匿名化の希望','確認事項','写真を撮ったのは次のうちどのエリアですか？',
                         'この写真にタイトルをつけてください','対面ワークショップに参加しましたか？','メールアドレス']
    new_name_submit = ['timestamp','name','photo_url','place','reason','anonimization',
                       'agreement','area','tittle','ws','email_adress']
    assert list(df.columns) == check_list_submit, 'フォームの質問事項が変更された可能性があります'
    df.columns = new_name_submit
    return df

  def numerication_submit(self,df):
    """質問事項の回答を数値化する"""
    df =  df.replace({'anonimization':{'':0,'公開を希望しない':1},
                      'agreement':{'':0,'上記の確認事項を確認し，同意する．':1}})
    return df.astype({'anonimization':'int8','agreement':'int8'})
  
  def numerication_submit_area(self,df):
    """エリア分けの回答を数値化する"""
    df =  df.replace({'area':{'1: 正門・安田講堂エリア':1,'2: 赤門・総合図書館エリア':2,
                              '3: 三四郎池エリア':3, '4: 工学部広場エリア':4, '5: 工学部エリア':5,
                             '6: 御殿下グラウンドエリア':6,'7: 医学部・薬学部エリア':7,
                             '8: 東大病院エリア':8,'9: 農正門エリア':9,'10: 農学部3号館エリア':10,
                             '11: 浅野キャンパスエリア':11}})
    return df.astype({'area':'int8'})

  def drop_anonimized_submit(self,df):
    """匿名化希望の人を匿名化する"""
    df = df[df['agreement'] == 1]
    df = df[df['anonimization'] == 0]
    return df
    
  def append_id(self,df):
    """写真のidを取り出し，img_idという列を作る"""
    df['img_id'] = 0  # img_idという列を作る
    for i in range(len(df)):
      url = df['photo_url'].iloc[i]
      #正規表現でidを取り出しimg_id列に加える
      id = re.findall(re.escape('https://drive.google.com/open?id=')+'(.+)',url)
      img_id = id[0]
      df['img_id'].iloc[i] = img_id
    return df

  def embedded_url(self,df):
    """"埋め込み用URLを作る"""
    df['img_url'] = 0  # img_urlという列を作る
    for i in range(len(df)):
      img_url = 'https://drive.google.com/uc?id=' + df['img_id'].iloc[i] + '&.JPG'
      df['img_url'].iloc[i] = img_url
    return df

  ######
  #reg用のデータ整形メソッド
  ######
  def rename_columns_reg(self,df):
    """カラム名を英語に変換する，フォームの質問事項を変えたら変更する"""
    check_list_reg = ['タイムスタンプ','ニックネーム','身分','学内での身分','学内での所属',
                      '年齢','キャンパスに訪れた目的','今現在どれくらいの頻度でキャンパスに訪れますか?',
                      '普段どの程度写真を撮りますか？','普段はどのような時に写真を撮りますか？(複数回答可能)',
                      '確認事項','メールアドレス']
    new_name_reg = ['timestamp','name','off_on_campus','fucluty_or_student','affiliation',
                    'age','purpos','frequency_visit','frequency_photo','photo_taking','agreement',
                    'email']
    assert list(df.columns) == check_list_reg, 'フォームの質問事項が変更された可能性があります'
    df.columns = new_name_reg
    return df

  def numerication_reg(self,df):
    """質問事項の回答を数値化する"""
    df =  df.replace({'frequency_photo':{'全く写真を撮らない':1,'あまり写真を撮らない':2,'よく写真を撮る':3,'非常によく写真を撮る':4,},
                      'agreement':{'':0,'上記の説明を確認し，同意する．':1},'anonimization':{'':0,'公開を希望しない':1}
                      })
    return df.astype({'age':'int8',
                        'frequency_photo':'int8',
                        'agreement':'int8'})

