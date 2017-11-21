# -*- coding: utf-8 -*-
# VPN数据导入

import sys
import xdrlib
import datetime
import glob
import xlrd
import MySQLdb


def main():
    # 打开数据库连接
    con=MySQLdb.connect(host='10.10.10.171', user='root', passwd='mysql', db='base_ods', port=3306)
    cur=con.cursor()

    # 查找文件
    for filename in glob.glob("/home/mysqldba/data/IP地址列表/IP地址列表*.xls"):
        print "正在处理：%s"%(filename)
        # 打开excel文件
        data = xlrd.open_workbook(filename)
        # 获取表格
        table = data.sheets()[0]
        nrows = table.nrows

        for i in range(1, nrows):
            ip=table.cell(i,0).value
            create_time=xlrd.xldate.xldate_as_datetime(table.cell(i,1).value, 0).strftime('%Y-%m-%d %H:%M:%S')
            sql="REPLACE INTO base_ods.vpn_ip(ip, create_time) VALUES(%s, %s);"
            cur.execute(sql, (ip, create_time))

        # 提交
        con.commit()

    # 关闭数据库连接
    cur.close()
    con.close()

if __name__=="__main__":
    main()