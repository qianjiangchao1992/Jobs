import pandas as pd
import numpy as np
import DataBaseConn as DBC
# threeandfourusers=pd.read_excel(r'D:\job_xiaoe\job_20190103\三四五线城市客户.xlsx')
# usertuple=tuple(threeandfourusers.user_id.values.tolist())
educationappid=pd.read_excel(r'D:\job_xiaoe\job_20190103\教育子类appid.xlsx')
appidtuple=tuple(educationappid.app_id.values.tolist())
appidcategory=pd.read_excel(r'D:\job_xiaoe\job_20190102\店铺行业标签教育类.xlsx')
def oneTwoCityUser():
    Bussconn = DBC.DataBaseConnect('db_ex_business').BussinessDataBase()
    Busscurson = Bussconn.cursor()
    sql_user="select distinct user_id from t_users " \
        "where app_id in {} wx_province in ('北京','上海','重庆','天津') or wx_city in ('成都','杭州','武汉','苏州','西安','南京','郑州','长沙','沈阳','青岛','宁波','东莞','无锡') or wx_city in ('昆明', '大连', '厦门', '合肥', '佛山', '福州', '哈尔滨', '济南', '温州', '长春', '石家庄', '常州', '泉州', '南宁', '贵阳', '南昌', '南通', '金华', '徐州', '太原', '嘉兴', '烟台', '惠州', '保定', '台州', '中山', '绍兴', '乌鲁木齐', '潍坊', '兰州');".format(appidtuple)
    Busscurson.execute(sql_user)
    users=Busscurson.fetchall()
    usersdf=pd.DataFrame(np.array(users),columns=['user_id'])
    userstuple=tuple(usersdf.user_id.values.tolist())
    sql_datas='select app_id,user_id,product_id from t_orders where app_id in {0} and user_id in {1};'.format(appidtuple,userstuple)
    Busscurson.execute(sql_datas)
    data_all=Busscurson.fetchall()
    data_alldf=pd.DataFrame(np.array(data_all),columns=['app_id','user_id','product_id'])
    data_educations=data_alldf.merge(appidcategory,on=['app_id'])
    return data_educations
####参与课程的用户
result={}
def ZhuxueTool():
    sql_user = "select distinct user_id from t_users " \
               "where wx_province in ('北京','上海','重庆','天津') or wx_city in ('成都','杭州','武汉','苏州','西安','南京','郑州','长沙','沈阳','青岛','宁波','东莞','无锡') or wx_city in ('昆明', '大连', '厦门', '合肥', '佛山', '福州', '哈尔滨', '济南', '温州', '长春', '石家庄', '常州', '泉州', '南宁', '贵阳', '南昌', '南通', '金华', '徐州', '太原', '嘉兴', '烟台', '惠州', '保定', '台州', '中山', '绍兴', '乌鲁木齐', '潍坊', '兰州');"
###核心业务库
    Bussconn=DBC.DataBaseConnect('db_ex_business').BussinessDataBase()
    Busscurson=Bussconn.cursor()
    Busscurson.execute(sql_user)
    users = Busscurson.fetchall()
    usersdf = pd.DataFrame(np.array(users), columns=['user_id'])
    usertuple = tuple(usersdf.user_id.values.tolist())
    c_sql='select count(distinct user_id) from t_community_user where user_id in {}'.format(usertuple)
    ac_sql='select count(distinct user_id) from t_activity_actor where user_id in {}'.format(usertuple)
    q_sql='select count(distinct user_id) from t_que_question where user_id in {}'.format(usertuple)
    an_sql='select count(distinct answer_user_id) from t_exercises_answer where answer_user_id in {}'.format(usertuple)
    Busscurson.execute(c_sql)
    cdata=Busscurson.fetchall()[0]
    result['communit']=cdata
    Busscurson.execute(ac_sql)
    acdata=Busscurson.fetchall()[0]
    result['activity']=acdata
    # Busscurson.execute(q_sql)
    # qdata=Busscurson.fetchall()[0]
    # result['question']=qdata
    Busscurson.execute(an_sql)
    andata=Busscurson.fetchall()[0]
    result['answer']=andata
    Busscurson.close()
    Bussconn.close()
    ###次级业务库
    ##打卡##
    Subpriconn=DBC.DataBaseConnect('db_ex_punch_card').SubprimeBussinessDataBase()
    Subpricurson=Subpriconn.cursor()
    card_sql='select count(distinct user_id) from t_clock_actor_user where user_id in {}'.format(usertuple)
    Subpricurson.execute(card_sql)
    carddata=Subpricurson.fetchall()[0]
    result['card']=carddata
    Subpricurson.close()
    Subpriconn.close()
    ##测试互动##
    Subpriconn=DBC.DataBaseConnect('db_ex_evaluation').SubprimeBussinessDataBase()
    Subpricurson=Subpriconn.cursor()
    test_sql='select count(distinct user_id) from t_evaluation_user where user_id in {}'.format(usertuple)
    Subpricurson.execute(test_sql)
    testdata=Subpricurson.fetchall()[0]
    result['test']=testdata
    Subpricurson.close()
    Subpriconn.close()
    ##考试##
    Subpriconn=DBC.DataBaseConnect('db_ex_examination').SubprimeBussinessDataBase()
    Subpricurson=Subpriconn.cursor()
    exam_sql='select count(distinct user_id) from t_participate_exam_user where user_id in {}'.format(usertuple)
    Subpricurson.execute(exam_sql)
    examdata=Subpricurson.fetchall()[0]
    result['exam']=examdata
    Subpricurson.close()
    Subpriconn.close()
    ##表单##
    Subpriconn=DBC.DataBaseConnect('db_ex_forms').SubprimeBussinessDataBase()
    Subpricurson=Subpriconn.cursor()
    form_sql='select count(distinct user_id) from t_form_collections where user_id in {}'.format(usertuple)
    Subpricurson.execute(form_sql)
    formdata=Subpricurson.fetchall()[0]
    result['forms']=formdata
    Subpricurson.close()
    Subpriconn.close()
    ##证书##
    Subpriconn=DBC.DataBaseConnect('db_ex_certificate').SubprimeBussinessDataBase()
    Subpricurson=Subpriconn.cursor()
    certi_sql='select count(distinct user_id) from t_certificate_users where user_id in {}'.format(usertuple)
    Subpricurson.execute(certi_sql)
    certidata=Subpricurson.fetchall()[0]
    result['certificate']=certidata
    Subpricurson.close()
    Subpriconn.close()
    print(result)



