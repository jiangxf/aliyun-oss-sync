# aliyun-oss-sync
阿里云本地同步

编译后打jar包，  加cron任务定时check目录上传
*/5 * * * * /home/dev/oss-task/run.sh

run.sh
`nohup java -jar /home/dev/oss-task/ossupload.jar >> /home/dev/oss-task/upload.log 2>&1 &`

依赖：
lib/aliyun-sdk-oss-2.0.5.jar
lib/commons-codec-1.9.jar
lib/commons-logging-1.2.jar
lib/hamcrest-core-1.1.jar
lib/httpclient-4.4.jar
lib/httpcore-4.4.jar
lib/jdom-1.1.jar
lib/log4j-1.2.17.jar
    
