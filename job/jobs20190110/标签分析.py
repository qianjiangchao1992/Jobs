import pandas as pd
import numpy as np
orignal=pd.read_table(r'D:\job_xiaoe\job_20190110\prodata.txt',header=None,delimiter='\t',low_memory=False,na_values='null',encoding='utf-8')
write=pd.ExcelWriter(r'D:\job_xiaoe\job_20190110\最新标签数据.xlsx')
def getDataSummary():
    result=[]
    orignal.columns=['appid','productid','label1','label2','label3']
    validprodnumbers=orignal[orignal['label3'].notnull()]['productid'].count()
    validappnumbers=orignal[orignal['label3'].notnull()]['appid'].drop_duplicates().count()
    allprodnumbers=orignal['productid'].drop_duplicates().count()
    allappnumbers=orignal['appid'].drop_duplicates().count()
    result.append(['全部商品数目',allprodnumbers])
    result.append(['有标签商品数目', validprodnumbers])
    result.append(['全部店铺数目', allappnumbers])
    result.append(['全部打上标签店铺数目', validappnumbers])
    resultdf=pd.DataFrame(result,columns=['type','numbers'])
    resultdf.to_excel(write,sheet_name='数据总览')
    print('数据总览数据已成功导入')
def getLabelData():
    labeldata=orignal[orignal['label3'].notnull()]
    labeldatagy=labeldata.groupby(by=['label1','label2','label3'])['productid'].count()
    labeldatadf=pd.DataFrame(labeldatagy)
    labeldatadf.reset_index(inplace=True)
    labeldatadf.to_excel(write,sheet_name='标签数据汇总')
    print('标签汇总数据已成功导入')
def main():
    getDataSummary()
    getLabelData()
    write.save()
if __name__ == '__main__':
    main()


