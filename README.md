# aliyun-oss-sync
阿里云本地同步

编译后打jar包，  加cron任务定时check目录上传
*/5 * * * * /home/dev/oss-task/run.sh

run.sh
`nohup java -jar /home/dev/oss-task/ossupload.jar >> /home/dev/oss-task/upload.log 2>&1 &`
    
    
