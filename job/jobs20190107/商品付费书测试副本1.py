import pandas as pd
import numpy as np
import DataBaseConn as DBC
def getdata(dt):
    bussconn=DBC.DataBaseConnect('db_ex_business').BussinessDataBase()
    busscurson=bussconn.cursor()
    aliveconn=DBC.DataBaseConnect('db_ex_alive').AliveDataBase()
    alivecurson=aliveconn.cursor()
    sql_audio = 'select count(*) from t_audio where created_at<=' + '"{}"'.format(str(
        dt)) + 'and payment_type in (1,2) and title not in ("一分钟了解小鹅通（体验内容）","图文教程(体验内容，支付后可提现)","专栏教程","验证支付信息专用商品，购买后请删除","测试")'
    sql_image = 'select count(*) from t_image_text where created_at<=' + '"{}"'.format(str(
        dt)) + 'and payment_type in (1,2) and title not in ("一分钟了解小鹅通（体验内容）","图文教程(体验内容，支付后可提现)","专栏教程","验证支付信息专用商品，购买后请删除","测试")'
    sql_video = 'select count(*) from t_video where created_at<=' + '"{}"'.format(str(
        dt)) + 'and payment_type in (1,2) and title not in ("一分钟了解小鹅通（体验内容）","图文教程(体验内容，支付后可提现)","专栏教程","验证支付信息专用商品，购买后请删除","测试")'
    sql_ebook = 'select count(*) from t_ebook where created_at<=' + '"{}"'.format(str(
        dt)) + 'and payment_type in (1,2) and title not in ("一分钟了解小鹅通（体验内容）","图文教程(体验内容，支付后可提现)","专栏教程","验证支付信息专用商品，购买后请删除","测试")'
    sql_pay_product = 'select count(*) from t_pay_products where created_at<=' + '"{}"'.format(str(
        dt)) + 'and sell_type=1 and name not in ("一分钟了解小鹅通（体验内容）","图文教程(体验内容，支付后可提现)","专栏教程","验证支付信息专用商品，购买后请删除","测试")'
    sql_alive = 'select count(*) from t_alive where created_at<=' + '"{}"'.format(str(
        dt)) + 'and payment_type in (1,2) and title not in ("一分钟了解小鹅通（体验内容）","图文教程(体验内容，支付后可提现)","专栏教程","验证支付信息专用商品，购买后请删除","测试")'
###商品数
    busscurson.execute(sql_audio)
    data_audio=busscurson.fetchall()
    busscurson.execute(sql_image)
    data_image=busscurson.fetchall()
    busscurson.execute(sql_ebook)
    data_ebook=busscurson.fetchall()
    busscurson.execute(sql_video)
    data_video=busscurson.fetchall()
    busscurson.execute(sql_pay_product)
    data_pay_product=busscurson.fetchall()
    alivecurson.execute(sql_alive)
    data_alive=alivecurson.fetchall()
    print(data_audio[0][0],data_image[0][0],data_ebook[0][0],data_video[0][0],data_pay_product[0][0],data_alive[0][0])
    data_products=data_audio[0][0]+data_image[0][0]+data_ebook[0][0]+data_video[0][0]+data_pay_product[0][0]+data_alive[0][0]
###付费商品数目
    sql_pay_audio = 'select count(*) from t_audio where created_at<=' + '"{}"'.format(str(
        dt)) + 'and payment_type =2 and title not in ("一分钟了解小鹅通（体验内容）","图文教程(体验内容，支付后可提现)","专栏教程","验证支付信息专用商品，购买后请删除","测试")'
    sql_pay_image = 'select count(*) from t_image_text where created_at<=' + '"{}"'.format(str(
        dt)) + 'and payment_type =2 and title not in ("一分钟了解小鹅通（体验内容）","图文教程(体验内容，支付后可提现)","专栏教程","验证支付信息专用商品，购买后请删除","测试")'
    sql_pay_video = 'select count(*) from t_video where created_at<=' + '"{}"'.format(str(
        dt)) + 'and payment_type =2 and title not in ("一分钟了解小鹅通（体验内容）","图文教程(体验内容，支付后可提现)","专栏教程","验证支付信息专用商品，购买后请删除","测试")'
    sql_pay_ebook = 'select count(*) from t_ebook where created_at<=' + '"{}"'.format(str(
        dt)) + 'and payment_type =2 and title not in ("一分钟了解小鹅通（体验内容）","图文教程(体验内容，支付后可提现)","专栏教程","验证支付信息专用商品，购买后请删除","测试")'
    sql_pay_pay_product = 'select count(*) from t_pay_products where created_at<=' + '"{}"'.format(str(
        dt)) + 'and sell_type =1 and price>0 and name not in ("一分钟了解小鹅通（体验内容）","图文教程(体验内容，支付后可提现)","专栏教程","验证支付信息专用商品，购买后请删除","测试")'
    sql_pay_alive = 'select count(*) from t_alive where created_at<=' + '"{}"'.format(str(
        dt)) + 'and payment_type =2 and title not in ("一分钟了解小鹅通（体验内容）","图文教程(体验内容，支付后可提现)","专栏教程","验证支付信息专用商品，购买后请删除","测试")'
    busscurson.execute(sql_pay_audio)
    data_pay_audio=busscurson.fetchall()
    busscurson.execute(sql_pay_image)
    data_pay_image=busscurson.fetchall()
    busscurson.execute(sql_pay_ebook)
    data_pay_ebook=busscurson.fetchall()
    busscurson.execute(sql_pay_video)
    data_pay_video=busscurson.fetchall()
    busscurson.execute(sql_pay_pay_product)
    data_pay_pay_product=busscurson.fetchall()
    alivecurson.execute(sql_pay_alive)
    data_pay_alive=alivecurson.fetchall()
    print(data_pay_audio[0][0],data_pay_image[0][0],data_pay_ebook[0][0],data_pay_pay_product[0][0],data_pay_video[0][0],data_pay_alive[0][0])
    data_payproducts=data_pay_audio[0][0]+data_pay_image[0][0]+data_pay_ebook[0][0]+data_pay_pay_product[0][0]+data_pay_video[0][0]+data_pay_alive[0][0]
###有销量商品
    sql_orders = 'select count(distinct product_id) from t_orders where created_at<=' + '"{}"'.format(
        str(dt)) + 'and purchase_name not in ("一分钟了解小鹅通（体验内容）","图文教程(体验内容，支付后可提现)","专栏教程","验证支付信息专用商品，购买后请删除","测试")'
    busscurson.execute(sql_orders)
    sqlorders=busscurson.fetchall()
    busscurson.close()
    bussconn.close()
    alivecurson.close()
    aliveconn.close()
    return[dt,data_products,data_payproducts,sqlorders[0][0]]
def main():
    result=[]

    result.append(getdata('2016-01-31 23:59:59'))
    # data=pd.DataFrame(result,columns=['dt','products','payproducts','countproducts'])
    # data.to_excel(r'D:\job_xiaoe\job_20190107\productsresult_test.xlsx')
if __name__ == '__main__':
    main()


