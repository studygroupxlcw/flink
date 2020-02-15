# FLINK
## 学习目标
1、了解Flink的开发流程，并完成一个Demo开发  
2、了解Flink集群的部署，并自行部署一个集群（standalone/yarn） 
## DEMO
### 需求说明
词频统计功能，从多个文本文件中读取数据，每分钟输出一次单词当前所在文本的次数，单词过期时间为1小时。  
PS.   
单词当前所在文本是指该单词最后一次出现的文本。   
当发现单词出现在别的文本时，次数重新统计。  
输入数据格式:filename||value   
输出格式:filename:word:times
### 实现要求
1、分别用窗口的方式和非窗口的方式实现   
2、支持异常恢复（选做）   
### 估计会用到的Flink功能
1、水印（Watermark）  
2、状态管理（State）  
3、Window函数