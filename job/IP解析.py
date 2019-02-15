import requests
ipaddr=input("输入ip")
url="http://ip.taobao.com/service/getIpInfo.php?ip=%s"%ipaddr
urlobject=requests.get(url).text
print(urlobject)
print(type(urlobject))