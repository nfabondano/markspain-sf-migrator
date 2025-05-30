require('dotenv').config({ override: true });
const {
  S3Client,
  ListObjectsV2Command,
  HeadObjectCommand,
  CopyObjectCommand,
  DeleteObjectCommand
} = require('@aws-sdk/client-s3');

const s3 = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });
const BUCKET = process.env.S3_BUCKET;
const FOLDERS = process.env.S3_FOLDER.split(',');

async function objectExists(key) {
    try {
      await s3.send(new HeadObjectCommand({ Bucket: BUCKET, Key: key }));
      return true;
    } catch (err) {
      if (
        err.name === 'NotFound' ||
        err.name === 'NoSuchKey' ||
        err.$metadata?.httpStatusCode === 404
      ) {
        return false;
      }
      throw err;
    }
  }

async function processParent(folder) {
  let token = undefined;
  do {
    const listResp = await s3.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: `${folder}/`,
      ContinuationToken: token
    }));

    for (const obj of listResp.Contents || []) {
      const parts = obj.Key.split('/');
      // skip top-level and direct files
      if (parts.length !== 4) continue;

      const parentPath = `${parts[0]}/${parts[1]}`;  // e.g. folder/parentId
      const subfolder = parts[2];                     // e.g. id_baseName
      const fileName = parts[3];                      // actual file
      const destKey = `${parentPath}/${fileName}`;

      if (!await objectExists(destKey)) {
        // move: copy then delete
        await s3.send(new CopyObjectCommand({
            Bucket: BUCKET,
            CopySource: `${BUCKET}/${obj.Key
              .split('/')
              .map(encodeURIComponent)
              .join('/')}`,
            Key: destKey
          }));
        await s3.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: obj.Key }));
        console.log(`Moved ${obj.Key} → ${destKey}`);
      } else {
        console.log(`Skipped ${obj.Key}: ${destKey} already exists`);
      }
    }

    token = listResp.IsTruncated ? listResp.NextContinuationToken : undefined;
  } while (token);
}

(async () => {
  for (const folder of FOLDERS) {
    console.log(`Processing folder: ${folder}`);
    await processParent(folder);
  }
  console.log('Cleanup complete');
})();
