'''
1.店铺--标签:
手动打的一套标签，kind t_joined_extra
CRM-销售标签
认证公司名称
2.店铺--地域
IP—
认证的公司名称—
3.用户-地域
微信x
IP—
'''
import DataBaseConn as DBC
import pandas as pd
import numpy as np
#第一步提取标准版店铺基本信息，地域，标签
def getStandardShopInfo():
    try:
        Oserconn=DBC.DataBaseConnect('db_ex_chain').O_ServeseWxbDataBase()
        Osercurson=Oserconn.cursor()
        sql='select app_id,predicted_country,predicted_region,predicted_city,kind from t_joined_extra where version_type in (3,4);'
        Osercurson.execute(sql)
        standshopdata =Osercurson.fetchall()
        standshopdatadf=pd.DataFrame(np.array(standshopdata),columns=['app_id','country','province','city','kindlabel'])
        # standshopdatadf.to_excel(r'D:\job_xiaoe\job_20181229\标准店铺基本信息.xlsx')
        return standshopdatadf
    except Exception as e:
        print(e)
        return []
def getCrmLabel():
    try:
        Oserconn = DBC.DataBaseConnect('db_ex_tags').O_ServeseWxbDataBase()
        Osercurson = Oserconn.cursor()
        sql='select t1.relation_id,t2.tag_name from (select relation_id,tag_id from t_tag_entitys_items where relation_id RLIKE \"^app\") t1 join (select id,tag_name from t_tag_items) t2 on(t1.tag_id=t2.id) where t2.tag_name is not NULL;'
        Osercurson.execute(sql)
        crmlabeldata=Osercurson.fetchall()
        crmlabeldatadf=pd.DataFrame(np.array(crmlabeldata),columns=['app_id','crmlabel'])
        # crmlabeldatadf.to_excel(r'D:\job_xiaoe\job_20181229\CRM系统标签.xlsx')
        return crmlabeldatadf
    except Exception as e:
        print(e)
        return []
def getAuthenticLabel():
    try:
        Subpriconn=DBC.DataBaseConnect('db_ex_shop_authentic').SubprimeBussinessDataBase()
        Subpricurson=Subpriconn.cursor()
        sql='select t1.app_id,t2.corporation_name,t2.identity_type from t_authentic_shop t1 join t_authentic t2 on (t1.authentic_id=t2.authentic_id) where t2.identity_type in (1,2);'
        Subpricurson.execute(sql)
        authenticlabeldata=Subpricurson.fetchall()
        authenticlabeldatadf =pd.DataFrame(np.array(authenticlabeldata),columns=['app_id','corporation_name','type'])
        # authenticlabeldatadf.to_excel(r'D:\job_xiaoe\job_20181229\公司认证标签.xlsx')
        return authenticlabeldatadf
    except Exception as e:
        print(e)
        return []
def getTopShop():
    try:
        Bussconn=DBC.DataBaseConnect('db_ex_business').BussinessDataBase()
        Busscurson=Bussconn.cursor()
        data=getStandardShopInfo()
        standdata=tuple(data['app_id'].values.tolist())
        sql='select app_id,product_id,count(*)as nums from t_orders where app_id in {} group by app_id,product_id having count(*)>1 order by nums desc'.format(standdata)
        Busscurson.execute(sql)
        topshopdata =Busscurson.fetchall()
        topshopdatadf=pd.DataFrame(np.array(topshopdata),columns=['app_id','product_id','numbers'])
        topshopdatadf.to_excel(r'D:\job_xiaoe\job_20181229\标准店铺商品信息.xlsx')
        return topshopdatadf
    except Exception as e:
        print(e)
        return []
def getAllProducts():
    #所有数据
    products=pd.read_excel(r'D:\job_xiaoe\job_20190102\product_classify.xlsx')
    #视频数据
    video=products[products['type']=='v']['product_id'].values.tolist()
    video=tuple(video)
    #会员专栏
    payproduct = products[products['type'] == 'p']['product_id'].values.tolist()
    payproduct = tuple(payproduct)
    #音频数据
    audio = products[products['type'] == 'a']['product_id'].values.tolist()
    audio = tuple(audio)
    #电子书数据
    ebook = products[products['type'] == 'e']['product_id'].values.tolist()
    ebook = tuple(ebook)
    #图文数据
    image = products[products['type'] == 'i']['product_id'].values.tolist()
    image = tuple(image)
    #问答数据
    que_products = products[products['type'] == 'q']['product_id'].values.tolist()
    que_products = tuple(que_products)
    #社群数据
    community = products[products['type'] == 'c']['product_id'].values.tolist()
    community = tuple(community)
    #超级会员
    svip = products[products['type'] == 's']['product_id'].values.tolist()
    svip = tuple(svip)
    #直播数据
    alive = products[products['type'] == 'l']['product_id'].values.tolist()
    alive = tuple(alive)
    # print(alive)
    #业务库所有所有商品
    try:
        Bussconn = DBC.DataBaseConnect('db_ex_business').BussinessDataBase()
        Busscurson = Bussconn.cursor()
        sql_video='select id,title from t_video where id in {};'.format(video)
        sql_audio='select id,title from t_audio where id in {};'.format(audio)
        sql_image='select id,title from t_image_text where id in {};'.format(image)
        sql_ebook='select id,title from t_ebook where id in {};'.format(ebook)
        sql_payproduct='select id,concat(name,summary) from t_pay_products where id in {};'.format(payproduct)
        sql_question='select id,concat(title,desc) from t_que_products where id in {};'.format(que_products)
        sql_community='select id,concat(title,describe) from t_community where id in {};'.format(community)
        sql_bussiness=[sql_video,sql_audio,sql_image,sql_ebook,sql_payproduct,sql_question,sql_community]
        result=pd.DataFrame()
        for sql in sql_bussiness:
            print(sql)
            # Busscurson.execute(sql)
            # data=Busscurson.fetchall()
            # df=pd.DataFrame(np.array(data),columns=['product','desc'])
            # result=result.append(df)
        Busscurson.close()
        Bussconn.close()
        # Aliveconn=DBC.DataBaseConnect('db_ex_alive').AliveDataBase()
        # Alivecurson=Aliveconn.cursor()
        Subprimconn=DBC.DataBaseConnect('db_ex_svip').SubprimeBussinessDataBase()
        Subprimcurson=Subprimconn.cursor()
        sql_svip='select id,concat(title,description) from t_svip where id in {};'.format(svip)
        svipdata=Subprimcurson.execute(sql_svip)
        Subprimcurson.execute(sql_svip)
        svipdata=Subprimcurson.fetchall()
        svipdatadf=pd.DataFrame(np.array(svipdata),columns=['product','desc'])
        print(svipdatadf)
        result=result.append(svipdatadf)
        print(len(result))
        result.to_excel('D:\job_xiaoe\job_20190102\data_result.xlsx')
        Subprimcurson.close()
        Subprimconn.close()
    except Exception as e:
        print(e)
def main():
    # getStandardShopInfo()
    # getCrmLabel()
    # getAuthenticLabel()
    # getTopShop()
    getAllProducts()
if __name__ == '__main__':
    main()

