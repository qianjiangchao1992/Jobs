import requests
from bs4 import BeautifulSoup
from collections import OrderedDict
import time
import re
import requests
import pandas as pd
import numpy as np
#获取第一页的内容
class ApplePPZhushou_Spyder():
    def __init__(self,url,savepath):
        self.url=url
        self.savepath=savepath
    @staticmethod
    def get_one_page(url):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
        }
        # cookies = '__f_=1541387456009;_ntes_nnid=44d1fcf3212edb75e093e1b0e0c0c1a0,1541387456827; _ntes_nuid=44d1fcf3212edb75e093e1b0e0c0c1a0; vjuids=-74dd4525.167501a9a3f.0.cf18ed686c95f; UM_distinctid=16781371900bee-0eaaa3c3ae282d-5c11301c-1fa400-16781371901842; vjlast=1543237180.1544061131.13; __gads=ID=6cad044ae32f6f69:T=1544061136:S=ALNI_Ma_OjEugvFqOHJcmfmqhGcZf8gAow; vinfo_n_f_l_n3=7960d450a30d852f.1.2.1543237179984.1544061262191.1544068316038; EDUWEBDEVICE=4a7a3cc2eb7c4f28b4e3bc3a63518a8a; hb_MA-BFF5-63705950A31C_source=www.baidu.com; hasVolume=true; videoVolume=0.8; NTES_PASSPORT=VRO_tJshdgyfzxycUENzI0ByYikXQ6Rabf3oyf6BHPvTbX4tbOxW2SBQOh7VmM1MPQcb26v.akPLpffhxJgPtkACZREApey9j3taGtgXPGRG9hDesb68uty0Th8p_tt2v.vXWhLgUW7jMD0Gfx8IAW2kNCMUx_Wcg08HD09jWIdHQjMIakK43W1AW; P_INFO=qjchao1992@163.com|1545114787|1|study|00&99|gud&1545056177&study#gud&440300#10#0#0|188209&0|study_client&study|qjchao1992@163.com; STUDY_NOT_SHOW_PROMOTION_WIN=true; sideBarPost=769; 1020603956=1020603956; NTESSTUDYSI=3c66cf348da947ba8a737ce9d8eaf55e; STUDY_INFO="qjchao1992@163.com|-1|1020603956|1545373364763"; STUDY_SESS="QuFZYfOLny/fk8JL3SbK5a1zxbwNOhsDqyG5VIex6hosNSuPeLisY8M4ct7Q8qzNpdNOnxdS5VTMde1d7ynOxYovmP4dS5b0tbsFcnSwvMjAUVwi2FNUn6gvRdB7iJl/zW42ZC2AtN4w1dXmQgAwEkNQPVE0YWBX9T/w7/joJpMvhQFx7kzH+3GA01euhE5D"; STUDY_PERSIST="H+5UimOUk0LA+5GT9AJxaIqgzfnVigKLLkrjxkHdRQS9hP1mCcCRutgZgtoPMpV1jO0dGn3zrmXsz0dMurPoqEiZB0uTepaWKctVaEinx6XnaoB/txHegLNN0I9jzllj8pPmwbMuoSEgFMz0sKHHc3IaZknPEDjVJo446mHWF+QKPeI3WnHQcdv6q6qGjBTF52dLVVH+8/vYFAFbvQMpVhPhfSS5WkxX68ygkjZch5y37Weq4j/S2qU01oFrl7Ms8WQLi3xTJ45sq/acjsEWiA=="; NETEASE_WDA_UID=1020603956#|#1475316846063; NTES_STUDY_YUNXIN_ACCID=s-1020603956; NTES_STUDY_YUNXIN_TOKEN=454b9cd8581fe378898d50793434707d; __utma=129633230.220939977.1545047951.1545139864.1545373368.5; __utmc=129633230; __utmz=129633230.1545373368.5.2.utmcsr=baidu|utmccn=affiliate|utmcmd=cpc|utmctr=sixth_anniversary04|utmcct=SEM; hb_MA-BFF5-63705950A31C_u=%7B%22utm_source%22%3A%20%22baidu%22%2C%22utm_medium%22%3A%20%22cpc%22%2C%22utm_campaign%22%3A%20%22affiliate%22%2C%22utm_content%22%3A%20%22SEM%22%2C%22utm_term%22%3A%20%22sixth_anniversary04%22%2C%22promotional_id%22%3A%20%22%22%7D; utm=eyJjIjoiIiwiY3QiOiIiLCJpIjoiIiwibSI6IiIsInMiOiIiLCJ0IjoiIn0=|aHR0cHM6Ly9zdHVkeS4xNjMuY29tL3RvcGljcy9zaXh0aF9hbm5pdmVyc2FyeS8/dXRtX3NvdXJjZT1iYWlkdSZ1dG1fbWVkaXVtPWNwYyZ1dG1fY2FtcGFpZ249YWZmaWxpYXRlJnV0bV90ZXJtPXNpeHRoX2Fubml2ZXJzYXJ5MDQmdXRtX2NvbnRlbnQ9U0VN; __utmb=129633230.15.9.1545373429510'
        # cookies='__f_=1541387456009; _ntes_nnid=44d1fcf3212edb75e093e1b0e0c0c1a0,1541387456827; _ntes_nuid=44d1fcf3212edb75e093e1b0e0c0c1a0; vjuids=-74dd4525.167501a9a3f.0.cf18ed686c95f; UM_distinctid=16781371900bee-0eaaa3c3ae282d-5c11301c-1fa400-16781371901842; vjlast=1543237180.1544061131.13; __gads=ID=6cad044ae32f6f69:T=1544061136:S=ALNI_Ma_OjEugvFqOHJcmfmqhGcZf8gAow; vinfo_n_f_l_n3=7960d450a30d852f.1.2.1543237179984.1544061262191.1544068316038; EDUWEBDEVICE=4a7a3cc2eb7c4f28b4e3bc3a63518a8a; hb_MA-BFF5-63705950A31C_source=www.baidu.com; hasVolume=true; videoVolume=0.8; STUDY_NOT_SHOW_PROMOTION_WIN=true; 1020603956=1020603956; hb_MA-BFF5-63705950A31C_u=%7B%22utm_source%22%3A%20%22baidu%22%2C%22utm_medium%22%3A%20%22cpc%22%2C%22utm_campaign%22%3A%20%22affiliate%22%2C%22utm_content%22%3A%20%22SEM%22%2C%22utm_term%22%3A%20%22sixth_anniversary04%22%2C%22promotional_id%22%3A%20%22%22%7D; __utmc=129633230; __utmz=129633230.1545619697.10.4.utmcsr=baidu|utmccn=affiliate|utmcmd=cpc|utmctr=sixth_anniversary04|utmcct=SEM; sideBarPost=772; __utma=129633230.220939977.1545047951.1545632183.1545623940.12; utm=eyJjIjoiIiwiY3QiOiIiLCJpIjoiIiwibSI6IiIsInMiOiIiLCJ0IjoiIn0=|aHR0cHM6Ly9zdHVkeS4xNjMuY29tLw==; NTESSTUDYSI=fd9d5ce0f7a94501b77dcbb14f9cca51; __utmb=129633230.7.8.1545644491055'
        # cookies='__f_=1541387456009; _ntes_nnid=44d1fcf3212edb75e093e1b0e0c0c1a0,1541387456827; _ntes_nuid=44d1fcf3212edb75e093e1b0e0c0c1a0; vjuids=-74dd4525.167501a9a3f.0.cf18ed686c95f; UM_distinctid=16781371900bee-0eaaa3c3ae282d-5c11301c-1fa400-16781371901842; vjlast=1543237180.1544061131.13; __gads=ID=6cad044ae32f6f69:T=1544061136:S=ALNI_Ma_OjEugvFqOHJcmfmqhGcZf8gAow; vinfo_n_f_l_n3=7960d450a30d852f.1.2.1543237179984.1544061262191.1544068316038; EDUWEBDEVICE=4a7a3cc2eb7c4f28b4e3bc3a63518a8a; hb_MA-BFF5-63705950A31C_source=www.baidu.com; hasVolume=true; videoVolume=0.8; STUDY_NOT_SHOW_PROMOTION_WIN=true; 1020603956=1020603956; hb_MA-BFF5-63705950A31C_u=%7B%22utm_source%22%3A%20%22baidu%22%2C%22utm_medium%22%3A%20%22cpc%22%2C%22utm_campaign%22%3A%20%22affiliate%22%2C%22utm_content%22%3A%20%22SEM%22%2C%22utm_term%22%3A%20%22sixth_anniversary04%22%2C%22promotional_id%22%3A%20%22%22%7D; __utmc=129633230; __utmz=129633230.1545619697.10.4.utmcsr=baidu|utmccn=affiliate|utmcmd=cpc|utmctr=sixth_anniversary04|utmcct=SEM; sideBarPost=772; __utma=129633230.220939977.1545047951.1545632183.1545623940.12; utm=eyJjIjoiIiwiY3QiOiIiLCJpIjoiIiwibSI6IiIsInMiOiIiLCJ0IjoiIn0=|aHR0cHM6Ly9zdHVkeS4xNjMuY29tLw==; NTESSTUDYSI=fd9d5ce0f7a94501b77dcbb14f9cca51; NTES_SESS=XigGE7yfwCyVIYRoeKw2F3EwkvRE0WdTGL4AzcbHPE7MwsISwc4gjpQ7cNdaxnenYJ2ebAyiIxPg40ZEC_3amLlsHfUxw4E8Rtics2heTI6xFZnsENmytPOOO_X88MV8ZDBOUHuruZYEEK31BGzXpL4Hus4xmft0nGg2NQYdNaVllfPMOgDifP3WpxjpdsXNRM1_UCSxgAMHibEGB17k21S_3; NTES_PASSPORT=yFbteSxrpeqWXU9IAGXg8wZPBlrA2jeANG64Jy8GK8oJpvFRpWZ9_iEbWdwyechckbgp_D8f2XkncqUNkpCQX9wSv5i5LOVRjKiazijzhNuXpDe4or6WfnoZ2Cr4d40czS29ZMem5o11taQYM9xKLDVnVF3cGBjFNrI81sqyXvOuB8hcVflXpgxT9; S_INFO=1545645351|0|3&80##|qjchao1992; P_INFO=qjchao1992@163.com|1545645351|1|study|00&99|gud&1545379449&study#gud&440300#10#0#0|188209&0|study_client&study|qjchao1992@163.com; STUDY_INFO="qjchao1992@163.com|-1|1020603956|1545645351647"; STUDY_SESS="QuFZYfOLny/fk8JL3SbK5a1zxbwNOhsDqyG5VIex6hosNSuPeLisY8M4ct7Q8qzNuc605iMcWlGTUxE1uRvMgQUg6mCNc9fkiQVrsJN3Ai44BTYEcFs+3nuLXqDwJEHajBVuYGSRVdAK9uPX6q5rretCdhTr0OYthYzPk8qORvUvhQFx7kzH+3GA01euhE5D"; STUDY_PERSIST="H+5UimOUk0LA+5GT9AJxaIqgzfnVigKLLkrjxkHdRQS9hP1mCcCRutgZgtoPMpV1jO0dGn3zrmXsz0dMurPoqIkA1gKkqhUix8s0azaYZ9Y+AzU/FYlz4LfHgOqHu41MhpQYjCwQxJxsiUSSTypurkkXYCgKyUEou3xYnCxfnCkZrFyIwVc7YW/zbyLlMEtRDcr1gnWB+bizg6F877hbdNhvAyAE2FcAsa1WHpDfE7237Weq4j/S2qU01oFrl7Ms8WQLi3xTJ45sq/acjsEWiA=="; NETEASE_WDA_UID=1020603956#|#1475316846063; NTES_STUDY_YUNXIN_ACCID=s-1020603956; NTES_STUDY_YUNXIN_TOKEN=454b9cd8581fe378898d50793434707d; __utmb=129633230.20.9.1545645356460'
        cookies='__f_=1541387456009; _ntes_nnid=44d1fcf3212edb75e093e1b0e0c0c1a0,1541387456827; _ntes_nuid=44d1fcf3212edb75e093e1b0e0c0c1a0; vjuids=-74dd4525.167501a9a3f.0.cf18ed686c95f; UM_distinctid=16781371900bee-0eaaa3c3ae282d-5c11301c-1fa400-16781371901842; vjlast=1543237180.1544061131.13; __gads=ID=6cad044ae32f6f69:T=1544061136:S=ALNI_Ma_OjEugvFqOHJcmfmqhGcZf8gAow; vinfo_n_f_l_n3=7960d450a30d852f.1.2.1543237179984.1544061262191.1544068316038; EDUWEBDEVICE=4a7a3cc2eb7c4f28b4e3bc3a63518a8a; hb_MA-BFF5-63705950A31C_source=www.baidu.com; hasVolume=true; videoVolume=0.8; STUDY_NOT_SHOW_PROMOTION_WIN=true; 1020603956=1020603956; hb_MA-BFF5-63705950A31C_u=%7B%22utm_source%22%3A%20%22baidu%22%2C%22utm_medium%22%3A%20%22cpc%22%2C%22utm_campaign%22%3A%20%22affiliate%22%2C%22utm_content%22%3A%20%22SEM%22%2C%22utm_term%22%3A%20%22sixth_anniversary04%22%2C%22promotional_id%22%3A%20%22%22%7D; __utmz=129633230.1545619697.10.4.utmcsr=baidu|utmccn=affiliate|utmcmd=cpc|utmctr=sixth_anniversary04|utmcct=SEM; sideBarPost=772; __utma=129633230.220939977.1545047951.1545703300.1545703370.16; utm=eyJjIjoiIiwiY3QiOiIiLCJpIjoiIiwibSI6IiIsInMiOiIiLCJ0IjoiIn0=|aHR0cHM6Ly9zdHVkeS4xNjMuY29tL2NhdGVnb3J5L3BwdA==; __utmc=129633230; NTESSTUDYSI=8b3dbe71bcd343d0b56dfdfd403ff8e3; NTES_SESS=5neTpf2D4AxhkG6lPtDT_1sCAJoq6RL1ecmfK2oEn.bBeypJekSXAhCmkuxMtYrYGUcr1LO7ptHXSZR8KgaM_V4ybj2teS83QT7kycfrspdtIRP5ekdOAxfvM8BgXDvfouojdtdBJrhmRjBQzIduo6IphVK7s.amCyUhKrhxamocXfBR1dkoq4HIcBnxkQclAqT4Vq2_aFw4E_OFyo3ZsaJga; NTES_PASSPORT=9CcgFkuDiv1_VVwJTf2Rk8Uv3QkYlAJIA2gUMzLkWj4x_q13_H8IY.y2HBjZAigi72G_YuOXFw7pitoT7_MawIqFdcyo0kMh91tTI.xyu_Utf_mPtgiPqLUE0bT65HkTzmYqmwZjHBIr.iDpCOYfHwgUe3kq_SMwJkacIpyd5b.7T; S_INFO=1545707291|0|3&80##|qjchao1992; P_INFO=qjchao1992@163.com|1545707291|1|study|00&99|gud&1545645351&study#gud&440300#10#0#0|188209&0|study_client&study|qjchao1992@163.com; STUDY_INFO="qjchao1992@163.com|-1|1020603956|1545707291177"; STUDY_SESS="QuFZYfOLny/fk8JL3SbK5a1zxbwNOhsDqyG5VIex6hosNSuPeLisY8M4ct7Q8qzN53bpXqKownp7sZoAwJdS6ADEDNmIRUJQuduPdePwW+8myWNd90UXk/cjc+Cntd3ZJ7+KptsipUY5GwvVu+QuR641wGEMnPxQuA43A+9uaTQvhQFx7kzH+3GA01euhE5D"; STUDY_PERSIST="H+5UimOUk0LA+5GT9AJxaIqgzfnVigKLLkrjxkHdRQS9hP1mCcCRutgZgtoPMpV1jO0dGn3zrmXsz0dMurPoqGfbmQbp1r8XxfE2Hn2i0yaxsLH/CO6AXODQ+qlXbVM2nJ4bIjygCUlxQ6GPbIRGNlFF5k3OUDhh9hE1b0djw4Fb7hEDcKl2fMvtmJqCqjKllMgH3WHfCaXOpFhVtG+sIc6PL0OqJUOe/Q0xJ3NMJrC37Weq4j/S2qU01oFrl7Ms8WQLi3xTJ45sq/acjsEWiA=="; NETEASE_WDA_UID=1020603956#|#1475316846063; NTES_STUDY_YUNXIN_ACCID=s-1020603956; NTES_STUDY_YUNXIN_TOKEN=454b9cd8581fe378898d50793434707d; __utmb=129633230.19.9.1545707326052'
        cookies_dict = {}
        print(url)
        for cookie in cookies.split(';'):
            k, v = cookie.split('=', 1)
            cookies_dict[k.strip()] = v.strip()
        response = requests.get(url, headers=headers, cookies=cookies_dict)
        if response.status_code == 200:
            return response.text
        return None
#解析第一页内容，数据结构化
    def parse_one_page(self):
        html = self.get_one_page(self.url)
        soup = BeautifulSoup(html,'lxml')
        label=[]
        # Labelpattle=re.compile('data-index=(.*)class=\"f-f0 first cat2 tit f-fl\"')
        # result=re.findall(Labelpattle,str(soup))
        for i in soup.find_all('a',class_="f-f0 first cat2 tit f-fl"):
            print(i)
            label01=re.compile(r'data-index=\"(.*?)\"')
            label02=re.compile(r'data-name=\"(.*?)\"')
            name01=re.findall(label01,str(i))[0].split('_')[0]
            name02=re.findall(label02,str(i))[0]
            label.append([name01,name02])
        return label

    def parse_two_page(self):
        html = self.get_one_page(self.url)
        soup = BeautifulSoup(html, 'lxml')
        label = []
        j=0
        for i in soup.find_all('p',class_="cate3links"):
            label01 = re.compile(r'data-index=\"(.*?)\"')
            label02 = re.compile(r'data-name=\"(.*?)\"')
            name01 = re.findall(label01, str(i))[0].split('_')[0]
            name02 = re.findall(label02, str(i))
            for k in (name02):
                label.append([j,name01,k])
            j+=1
        return label

    def parse_three_page(self):
        html = self.get_one_page(self.url)
        soup = BeautifulSoup(html, 'lxml')
        label = []
        j = 0
        for i in soup.find_all('li', class_="level-item"):
            label01 = re.compile(r'id=\"(.*?)\"')
            print(i)
            # label02 = re.compile(r'data-name=\"(.*?)\"')
            name01 = re.findall(label01, str(i))
            # label=[]
            if len(name01)>0:
                print(name01)
                name01=name01[0].split('-')[1]
                name02 = i.get_text().strip('\n')
                label.append([name01,name02])
            else:
                pass
        # print(label)
        return label
        #     for k in (name02):
        #         label.append([j, name01, k])
        #     j += 1
        # return label
        # print(result)
        # return result
#解析第二页内容，数据结构化
    # def parse_two_page(self):
    #     urls=self.parse_one_page()
    #     result=[]
    #     for index in urls.keys():
    #         catname=index
    #         print(urls[index])
    #         html=self.get_one_page(urls[index])
    #         soup=BeautifulSoup(html,'lxml')
    #         for i in soup.find_all('p',class_='cate3links'):
    #             print(i)
    #             pat1=re.compile('data-index=\"(.*?)\"')
    #             pat2=re.compile('data-name=\"(.*?)\"')
    #             pat3=re.compile('href=\"(.*?)\"')
    #             key1=re.findall(pat1,str(i))
    #             key11=re.findall(pat2,str(i))
    #             href=re.findall(pat3,str(i))
    #             for j in range(len(key1)):
    #                 result.append([key1[j].split('_')[0],key11[j],'https://study.163.com'+href[j]])
    #     resultdata=pd.DataFrame(result,columns=['Label01','Label03','href'])
    #     resultdata.to_excel(r'C:\Users\Administrator\Desktop\wangyicourse.xlsx')
    #     return result
    # def parse_three_page(self):
    #     datas=self.parse_two_page()
    #     for i in datas:
    #         key=i[1]
    #         url=i[2]
    #         html=self.get_one_page('https://study.163.com/category/ppt')
    #         soup=BeautifulSoup(html,'lxml')
    #         print(soup)
    #     urls=self.parse_one_page()
    #     result=[]
    #     for index in urls.keys():
    #         catname=index
    #         print(urls[index])
    #         html=self.get_one_page(urls[index])
    #         soup=BeautifulSoup(html,'lxml')
    #         for i in soup.find_all('p',class_='cate3links'):
    #             print(i)
    #             pat1=re.compile('data-index=\"(.*?)\"')
    #             pat2=re.compile('data-name=\"(.*?)\"')
    #             pat3=re.compile('href=\"(.*?)\"')
    #             key1=re.findall(pat1,str(i))
    #             key11=re.findall(pat2,str(i))
    #             href=re.findall(pat3,str(i))
    #             for j in range(len(key1)):
    #                 result.append([key1[j].split('_')[0],key11[j],'https://study.163.com'+href[j]])
    #     resultdata=pd.DataFrame(result,columns=['Label01','Label03','href'])
    #     resultdata.to_excel(r'C:\Users\Administrator\Desktop\wangyicourse.xlsx')
    #     return result
    # def write_txt(self):
    #     start_time=time.time()
    #     f=open(self.savepath,'w',encoding='utf-8')
    #     f.write('一级类目\t二级类目\tAPPname\n')
    #     for item in self.parse_two_page():
    #         f.write(item['first_cate']+'\t'+item['second_cate']+'\t'+item['APPNAME']+'\n')
    #     f.close()
    #     end_time=time.time()
    #     print('该程序运行时间为%s'%(end_time-start_time))
rt=ApplePPZhushou_Spyder('https://study.163.com/category/ppt',r'C:\Users\Administrator\Desktop\WangYiYunCourse_Spyder.txt')
# data=rt.parse_one_page()
# data2=rt.parse_two_page()
# # data1=rt.parse_two_page()
# # result=pd.DataFrame(data,columns=['一级类目','二级类目','三级类目'])
# # result.to_excel(r'C:\Users\Administrator\Desktop\WangYiYunCourseType.xlsx')
# result1=pd.DataFrame(data,columns=['一级类目','二级类目'])
# result2=pd.DataFrame(data2,columns=['序号','一级类目','三级类目'])
# print(result1)

def GetData(index,id,name):
    headers = {
        'Host': 'study.163.com',
        'accept': 'application/json',
        'origin': 'https://study.163.com',
        'edu-script-token': 'fcf568524429479585a49d78b0302145',
        'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36',
        'content-type': 'application/json',
        'accept-language': 'zh-CN,zh;q=0.',
    }

    data = '{"pageIndex":'+str(index)+',"pageSize":50,"relativeOffset":0,"frontCategoryId":'+id+',"searchTimeType":-1,"orderType":50,"priceType":-1,"activityId":0,"keyword":""}'
    resultnew = []
    try:
        response = requests.post('https://study.163.com/p/search/studycourse.json', headers=headers, data=data)
        result=response.text
        patt1=re.compile('productName\":\"(.*?)\"',re.I)
        patt2=re.compile('description\":\"(.*?)\"',re.I)
        productNames=re.findall(patt1,result)
        productdesc=re.findall(patt2,result)
        if len(productNames)>0:
            for i ,j in zip(productNames,productdesc):
                resultnew.append([name,i,j])
        else:
            pass
    except Exception as e:
        print(e)
        resultnew.append([name,'null','null'])
    return resultnew

def main():
    rt = ApplePPZhushou_Spyder('https://study.163.com/category/ppt',
                               r'C:\Users\Administrator\Desktop\WangYiYunCourse_Spyder.txt')
    data=rt.parse_three_page()
    dataresult=[]
    for i in data:
        id=i[0]
        name=i[1]
        for j in range(1,5):
            result=GetData(j,id,name)
            dataresult.extend(GetData(j,id,name))
            print(dataresult)
    dataresult=pd.DataFrame(dataresult,columns=['三级类目','名称','描述'])
    dataresult.to_excel(r'C:\Users\Administrator\Desktop\WangyiyunResult.xlsx')
if __name__ == '__main__':
    main()

