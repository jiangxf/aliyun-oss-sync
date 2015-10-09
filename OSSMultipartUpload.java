import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Base64;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;

/**
	放在235上，定时任务轮询上传视频文件目录
 **/
public class OSSMultipartUpload {

    private static final int _5min = 5*60*1000;//ms   监听超过5分钟无修改的文件，认为是完整的本地待上传文件
	private static final String ACCESS_ID = "xxx";
    private static final String ACCESS_KEY = "xxx";

    private static final String UPLOAD_FILE_PATH = "/mnt/vedio-upload/";    //本地待上传目录
    private static final String OSS_ENDPOINT = "http://oss-cn-qingdao.aliyuncs.com";

    private static final long PART_SIZE = 1 * 1024 * 1024L; // 每个Part的大小，最小为1MB
    private static final int CONCURRENCIES = 2; // 上传Part的并发线程数。

    
    private static final String BUCKET_NAME  = "xxx-video";
    
    /**
     * @param args
     */
	public static void main(String[] args) throws Exception {
		for (;;) {
			System.out.println("------------------\n -启动任务--当前时间：" + new Date().toLocaleString());
			// 可以使用ClientConfiguration对象设置代理服务器、最大重试次数等参数。
			// 列文件清单
			List<String> listnew = Arrays.asList(new File(UPLOAD_FILE_PATH).list());
			if (listnew == null || listnew.size() == 0) {
				System.out.println("无上传任务");
			} else {
				ClientConfiguration config = new ClientConfiguration();
				config.setMaxErrorRetry(5);
				config.setMaxConnections(10);
				OSSClient client = new OSSClient(OSS_ENDPOINT, ACCESS_ID, ACCESS_KEY);

				for (String key : listnew) { // 遍历上传
					System.out.println("************\n读取到本地文件 key:" + key);
					File uploadFile = new File(UPLOAD_FILE_PATH + key);
					if (new Date().getTime() - uploadFile.lastModified() > _5min) {//5分钟前修改的文件，认为是上传完毕的
						try {
							uploadFile(client, BUCKET_NAME, key, uploadFile);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
				client.shutdown();
			}
			System.out.println("-结束任务--当前时间：" + new Date().toLocaleString());
			Thread.sleep(_5min);
		}

    }

    
//	/**
//	 * 判断文件是否存在
//	 * @return
//	 */
//	public static boolean head (String key){
//		OSSClient client = new OSSClient("http://oss-cn-qingdao.aliyuncs.com", ACCESS_ID, ACCESS_KEY);
//        try {
//			ObjectMetadata objectMetadata = client.getObjectMetadata(BUCKET_NAME, key);
//			System.out.println(objectMetadata.getETag());
//			System.out.println(objectMetadata.getContentMD5());
//			System.out.println(objectMetadata.getContentLength());
//		} catch (OSSException e) {
//			if("NoSuchKey".equals(e.getErrorCode()) ){
//				return false;
//			}
//		} 
//        return true;
//	}

    
    public static void uploadFile(OSSClient client, String bucketName, String key,
            File uploadFile) throws OSSException, ClientException, InterruptedException {

        int partCount = calPartCount(uploadFile);
        if (partCount <= 1) {  //小1M的直接上传
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, uploadFile);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentMD5(md5File(new File(UPLOAD_FILE_PATH + key)));
			putObjectRequest.setMetadata(metadata );
			client.putObject(putObjectRequest);
            System.out.println("小于1M直接put上传");
            //完成后删除本地文件
            new File(UPLOAD_FILE_PATH+key).delete();            
            System.out.println("上传成功，删除本地文件 key:"+key);            
            return;
        }
        System.out.println("文件key:"+key+" 分片总数partCount:"+partCount);
        String uploadId = initMultipartUpload(client, bucketName, key);

        ExecutorService pool = Executors.newFixedThreadPool(CONCURRENCIES);

        List<PartETag> eTags = Collections.synchronizedList(new ArrayList<PartETag>());

        for (int i = 0; i < partCount; i++) {
            long start = PART_SIZE * i;
            long curPartSize = PART_SIZE < uploadFile.length() - start ?
                    PART_SIZE : uploadFile.length() - start;

            pool.execute(new UploadPartThread(client, bucketName, key,
                    uploadFile, uploadId, i + 1,
                    PART_SIZE * i, curPartSize, eTags));
        }

        pool.shutdown();
        while (!pool.isTerminated()) {
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }

        if (eTags.size() != partCount)
        {
            throw new IllegalStateException("Multipart上传失败，有Part未上传成功。 key="+key);
        }

        completeMultipartUpload(client, bucketName, key, uploadId, eTags);
    }

    // 根据文件的大小和每个Part的大小计算需要划分的Part个数。
    private static int calPartCount(File f) {
        int partCount = (int) (f.length() / PART_SIZE);
        if (f.length() % PART_SIZE != 0){
            partCount++;
        }
        return partCount;
    }

    // 初始化一个Multi-part upload请求。
    private static String initMultipartUpload(OSSClient client,
            String bucketName, String key) throws OSSException, ClientException {
        InitiateMultipartUploadRequest initUploadRequest =
                new InitiateMultipartUploadRequest(bucketName, key);
        InitiateMultipartUploadResult initResult =
                client.initiateMultipartUpload(initUploadRequest);
        String uploadId = initResult.getUploadId();
        return uploadId;
    }

    // 完成一个multi-part请求。
    private static void completeMultipartUpload(OSSClient client,
            String bucketName, String key, String uploadId, List<PartETag> eTags)
                    throws OSSException, ClientException {
        //为part按partnumber排序
        Collections.sort(eTags, new Comparator<PartETag>(){

            public int compare(PartETag arg0, PartETag arg1) {
                PartETag part1= arg0;
                PartETag part2= arg1;

                return part1.getPartNumber() - part2.getPartNumber();
            }  
        });

        CompleteMultipartUploadRequest completeMultipartUploadRequest =
                new CompleteMultipartUploadRequest(bucketName, key, uploadId, eTags);
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Content-MD5", md5File(new File(UPLOAD_FILE_PATH + key)));
		completeMultipartUploadRequest.setHeaders(headers);
		client.completeMultipartUpload(completeMultipartUploadRequest);
        //完成后删除本地文件
        new File(UPLOAD_FILE_PATH+key).delete();
        
        System.out.println("上传成功，删除本地文件 key:"+key);
    }

    private static class UploadPartThread implements Runnable {
        private File uploadFile;
        private String bucket;
        private String object;
        private long start;
        private long size;
        private List<PartETag> eTags;
        private int partId;
        private OSSClient client;
        private String uploadId;

        UploadPartThread(OSSClient client,String bucket, String object,
                File uploadFile,String uploadId, int partId,
                long start, long partSize, List<PartETag> eTags) {
            this.uploadFile = uploadFile;
            this.bucket = bucket;
            this.object = object;
            this.start = start;
            this.size = partSize;
            this.eTags = eTags;
            this.partId = partId;
            this.client = client;
            this.uploadId = uploadId;
        }

        @Override
        public void run() {

            InputStream in = null;
            try {
                in = new FileInputStream(uploadFile);
                in.skip(start);

                UploadPartRequest uploadPartRequest = new UploadPartRequest();
                uploadPartRequest.setBucketName(bucket);
                uploadPartRequest.setKey(object);
                uploadPartRequest.setUploadId(uploadId);
                uploadPartRequest.setInputStream(in);
                uploadPartRequest.setPartSize(size);
                uploadPartRequest.setPartNumber(partId);

                UploadPartResult uploadPartResult = client.uploadPart(uploadPartRequest);
                System.out.println("part-ok  partId="+partId);
                eTags.add(uploadPartResult.getPartETag());

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (in != null) try { in.close(); } catch (Exception e) {}
            }
        }
    }

    /**
     * 计算文件md5
     * @param f
     * @return  base64的md5 byte[]
     */
    public static String md5File(File f){
    	String md5 = "";
		try {
	        FileInputStream fis= new FileInputStream(f);    
			byte[] by = org.apache.commons.codec.digest.DigestUtils.md5(fis);
			md5 = Base64.encodeBase64String(by);
		} catch (IOException e) {
			e.printStackTrace();
		}
    	return md5;
    }
}

