import pandas as pd
import numpy as np
import jieba
import datetime
class Module():
    def __init__(self,originalpath,savepath):
        # self.tags=pd.read_table(r'D:\job_xiaoe\AutoLable\parent_son_tag.txt',delimiter='\t',encoding='utf-8')
        self.tags=pd.read_excel(r'D:\job_xiaoe\AutoLable\parent_son_tag.xlsx')
        self.tags=self.tags[['son', 'parent']]
        self.tagsdict=dict(self.tags.values.tolist())
        self.savepath=savepath
        self.datascore = pd.read_excel(r'D:\job_xiaoe\AutoLable\score\score.xlsx')
        self.datascore = self.datascore.round({'score': 4})
        self.originaldata=pd.read_excel(originalpath)
    def getPredictLabel(self):
        resource = self.originaldata.resource_id.values.tolist()
        resourceid = []
        for i, j in enumerate(resource):
            resourceid.append([j, i])
        resourcedf = pd.DataFrame(resourceid, columns=['resource_id', 'id'])
        resourcedf['id'] = resourcedf['id'].astype('int64')
        wordset = self.originaldata.comment.values.tolist()
        result = []
        for j, i in enumerate(wordset):
            if type(i) == str:
                words = list(jieba.cut(i, cut_all=False))
                result.append([j, i, self.getBetterLabel(words)[0], self.getBetterLabel(words)[1], self.getBetterLabel(words)[2],
                               self.getBetterLabel(words)[3], self.getBetterLabel(words)[4]])
            else:
                pass
        resultdataframe = pd.DataFrame(result, columns=['id', 'comment', 'LabelScore01', 'LabelScore02', 'LabelScore03',
                                                        'VoteLabel', 'score01'])
        resultdataframe['id'] = resultdataframe['id'].astype('int64')
        finalldata = resourcedf.merge(resultdataframe, on='id')
        finalldatafy = finalldata[
            ['resource_id', 'comment', 'LabelScore01', 'LabelScore02', 'LabelScore03', 'VoteLabel', 'score01']]
        finalldatafy.to_excel(self.savepath)
        print('successful!!!')
    def getBetterLabel(self,wordlist):
        wordlist = list(set(wordlist))
        datavalid = self.datascore[self.datascore.word.isin(wordlist)]
        datavalid = datavalid.groupby(by=['category'])['score'].sum()
        datavalid = pd.DataFrame(datavalid)
        datavalid.reset_index(inplace=True)
        datavalid.sort_values(by='score', ascending=False, inplace=True)
        data_three = datavalid.head(3)
        datadict = dict(list(data_three.values))
        print(datavalid.head(10))
        result =self.VoteType(datadict)
        return result
    def VoteType(self,dicts):
        keylist = list(dicts.keys())
        tagsvote = {}
        tags = self.tagsdict
        for i in keylist:
            tagsvote[i] = tags[i]
        # print(dicts)
        try:
            if dicts[keylist[0]] > 0.2:
                if dicts[keylist[0]] >= (dicts[keylist[1]] + dicts[keylist[2]]):
                    return [keylist[0], keylist[1], keylist[2], keylist[0], dicts[keylist[0]]]
                elif tagsvote[keylist[1]] == tagsvote[keylist[2]] and tagsvote[keylist[1]] != tagsvote[keylist[0]]:
                    return [keylist[0], keylist[1], keylist[2], keylist[1], dicts[keylist[0]],tagsvote[keylist[1]]]
                else:
                    return [keylist[0], keylist[1], keylist[2], keylist[0], dicts[keylist[0]],tagsvote[keylist[0]]]
            else:
                return ['null', 'null', 'null', 'null', 'null','null']
        except:
            return ['null', 'null', 'null', 'null', 'null','null']

def main():
    starttime=datetime.datetime.now()
    for i in range(1):
        originalpath=r'D:\job_xiaoe\AutoLable\newresult\test1.xlsx'
        savepath=r'D:\job_xiaoe\AutoLable\newresult\test_result1.xlsx'
        MD=Module(originalpath,savepath)
        MD.getPredictLabel()
    endtime=datetime.datetime.now()
    print('模型运行时间为:{}'.format((endtime-starttime).seconds))
if __name__ == '__main__':
    main()


