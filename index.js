require('dotenv').config({ override: true });
const {
  S3Client,
  PutObjectCommand,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  ListObjectsV2Command        // ← Added
} = require('@aws-sdk/client-s3'); const { NodeHttpHandler } = require('@aws-sdk/node-http-handler');
const { DynamoDBClient, GetItemCommand, PutItemCommand } = require('@aws-sdk/client-dynamodb');
const jsforce = require('jsforce');
const fetch = require('node-fetch');
const fs = require('fs');
const path = require('path')

//Dates to filter
const filterDate = {
  startDate: process.env.START_DATE,//'2023-01-01T00:00:00Z',
  endDate: process.env.END_DATE//'2024-10-22T23:59:59Z'
};

// Constants for configurations
const S3_BUCKET = process.env.S3_BUCKET;
const S3_FOLDER = process.env.S3_FOLDER;
const DYNAMO_TABLE = process.env.DYNAMO_TABLE;
const MULTIPART_THRESHOLD = parseInt(process.env.MULTIPART_THRESHOLD, 10);  // Multipart Upload limit in bytes

// Salesforce credentials
const SF_USERNAME = process.env.SF_USERNAME;
const SF_PASSWORD = process.env.SF_PASSWORD;

// Ruta al archivo de log
const logFilePath = path.join(__dirname, 'migration_errors.log');

// AWS SDK configuration
const s3 = new S3Client({
  region: process.env.AWS_REGION || 'us-east-1',
  requestHandler: new NodeHttpHandler({
    connectionTimeout: 30000,  // 30 seconds timeout
    socketTimeout: 300000,     // 5 minutes socket timeout
    maxSockets: 150            // Increase max sockets to 150
  })
});

const dynamoDB = new DynamoDBClient({ region: 'us-east-1' });

// Create a connection to Salesforce
const conn = new jsforce.Connection({
  loginUrl: process.env.SF_URL  // Salesforce sandbox URL
});

// Function to check if the file already exists in DynamoDB
async function fileExistsInDynamoDB(fileName) {
  const params = {
    TableName: DYNAMO_TABLE,
    Key: {
      fileName: { S: fileName }
    }
  };
  try {
    const result = await dynamoDB.send(new GetItemCommand(params));
    return !!result.Item;
  } catch (err) {
    console.error('Error checking DynamoDB for file:', err);
    throw err;
  }
}

function logErrorToFile(message) {
  const timestamp = new Date().toISOString(); // Timestamp para el log
  const logMessage = `[${timestamp}] ${message}\n`;
  fs.appendFileSync(logFilePath, logMessage, 'utf8');
}

// Function to handle Multipart Upload for large files with progress tracking
async function uploadLargeFileToS3(fileName, buffer) {
  try {
    const multipartUpload = await s3.send(new CreateMultipartUploadCommand({
      Bucket: S3_BUCKET,
      Key: fileName,
    }));

    const partSize = 5 * 1024 * 1024; // 5 MB per part
    const parts = Math.ceil(buffer.length / partSize);
    const uploadResults = [];
    let uploadedSize = 0;  // Track the total uploaded size

    console.log(`Starting upload of file: ${fileName} in ${parts} parts...`);

    // Upload each part and store the ETag
    for (let i = 0; i < parts; i++) {
      const partParams = {
        Bucket: S3_BUCKET,
        Key: fileName,
        PartNumber: i + 1,
        UploadId: multipartUpload.UploadId,
        Body: buffer.slice(i * partSize, (i + 1) * partSize)
      };

      // Send the part upload request
      const uploadResult = await s3.send(new UploadPartCommand(partParams));

      // Track the size uploaded
      uploadedSize += partParams.Body.length / 1024;  // Convert bytes to kilobytes
      process.stdout.write(`\rUploaded ${i}/${parts}: ${uploadedSize.toFixed(2)} KB`);

      // Store the ETag and part number for later use
      uploadResults.push({
        ETag: uploadResult.ETag,
        PartNumber: i + 1
      });
    }

    console.log(`\nUpload of ${fileName} completed. Total uploaded: ${uploadedSize.toFixed(2)} KB`);

    // Complete the multipart upload with the ETags and part numbers
    const completeParams = {
      Bucket: S3_BUCKET,
      Key: fileName,
      UploadId: multipartUpload.UploadId,
      MultipartUpload: {
        Parts: uploadResults
      }
    };

    await s3.send(new CompleteMultipartUploadCommand(completeParams));

    // Free the buffer after upload
    buffer = null;
    global.gc && global.gc();  // Force garbage collection if enabled manually

    console.log(`File ${fileName} successfully uploaded to S3 with multipart upload.`);
  } catch (error) {
    console.error(`Error in multipart upload for file ${fileName}:`, error);
    throw error;
  }
}

// Function to download file content using VersionData URL and track progress
async function downloadFile(versionDataUrl) {
  const headers = {
    'Authorization': `Bearer ${conn.accessToken}`
  };

  try {
    const fullUrl = `${conn.instanceUrl}${versionDataUrl}`;

    // Verify URL
    if (!fullUrl || typeof fullUrl !== 'string' || !fullUrl.startsWith('http')) {
      throw new Error(`Invalid URL detected: ${fullUrl}`);
    }

    console.log(`Attempting to download from URL: ${fullUrl}`);  // Print URL
    console.log(JSON.stringify(versionDataUrl))

    const response = await fetch(fullUrl, { headers });
    if (!response.ok) {
      throw new Error(`Failed to fetch file: ${response.statusText}`);
    }

    // Track the size of the downloaded content in kilobytes
    let downloadedSize = 0;
    const dataChunks = [];

    return new Promise((resolve, reject) => {
      response.body.on('data', chunk => {
        dataChunks.push(chunk);
        downloadedSize += chunk.length / 1024;  // Convert to kilobytes

        // Print the current downloaded size in kilobytes
        process.stdout.write(`\rDownloaded: ${downloadedSize.toFixed(2)} KB`);
      });

      response.body.on('end', () => {
        console.log(`\nDownload completed. Total downloaded: ${downloadedSize.toFixed(2)} KB`);
        resolve(Buffer.concat(dataChunks));  // Return the buffer containing the file data
      });

      response.body.on('error', (err) => {
        console.error('Error downloading file:', err);
        console.log('Object', versionDataUrl)
        logErrorToFile(`Error downloading file from URL ${fullUrl}: ${err.message}`);
        logErrorToFile(versionDataUrl);
        reject(err);
      });
    });
  } catch (err) {
    // Registra el error en el archivo de log y continúa
    console.error('Error downloading file:', err);
    console.log(versionDataUrl)
    logErrorToFile(`Error downloading file: ${err.message}`);
    logErrorToFile(versionDataUrl);
    return null;  // Retorna `null` para indicar que falló la descarga
  }
}


function buildQuery(filters) {
  console.log("FILTERS")
  console.log(filters)

  let baseQuery = `SELECT LinkedEntityId, Id, ContentDocument.CreatedDate ,ContentDocument.Title, ContentDocumentId, ContentDocument.FileType, ContentDocument.LatestPublishedVersion.FileExtension, ContentDocument.LatestPublishedVersion.VersionData 
  FROM ContentDocumentLink 
  WHERE LinkedEntityId IN (SELECT Id FROM ${S3_FOLDER}) 
  AND ContentDocument.FileType != 'SNOTE'`;

  // Agrega filtros de fecha si están presentes
  if (filters.startDate) {
    baseQuery += ` AND ContentDocument.CreatedDate >= ${filters.startDate}`;
  }

  if (filters.endDate) {
    baseQuery += ` AND ContentDocument.CreatedDate <= ${filters.endDate}`;
  }

  console.log("base query", baseQuery)

  return baseQuery;
}

// Example record processing function (download and upload)
async function processRecord(record) {
  let answer = false;

  const fileId = record.ContentDocumentId;
  const parentId = record.LinkedEntityId
  const title = record.ContentDocument.Title.replace(/\s+/g, '_'); // Reemplaza espacios en el título por guiones bajos
  const fileExtension = record.ContentDocument.LatestPublishedVersion.FileExtension.toLowerCase();
  const fileName = `${S3_FOLDER}/${parentId}/${title}.${fileExtension}`;
  const createdDate = record.ContentDocument.CreatedDate;
  const versionDataUrl = record.ContentDocument.LatestPublishedVersion.VersionData;

  console.log("------------------------------------------------------")
  // Verifica si el archivo ya existe en DynamoDB
  const fileExists = await fileExistsInDynamoDB(fileName);
  if (fileExists) {
    console.log(`File ${fileName} already exists. Skipping upload.`);
    return answer;
  }

  // Descarga el archivo
  console.log(`Downloading file ID: ${fileId} - ${createdDate} - ${fileName}`);
  const buffer = await downloadFile(versionDataUrl);

  // Si la descarga falló (buffer es null), salta al siguiente registro y registra el error
  if (!buffer) {
    logErrorToFile(`Failed to download file ${fileName} with URL: ${versionDataUrl}`);
    console.log(`Skipping file ${fileName} due to download failure.`);
    throw new Error(`Failed to download: ${fileName}`);
  }

  // Carga el archivo en S3
  try {
    if (buffer.length > MULTIPART_THRESHOLD) {
      console.log(`File ${fileName} is larger than 5MB, using Multipart Upload...`);
      await uploadLargeFileToS3(fileName, buffer);
    } else {
      console.log(`Uploading file ${fileName} to S3...`);
      const s3Params = {
        Bucket: S3_BUCKET,
        Key: fileName, // Usa la ruta con Opportunity/id para simular la estructura de carpetas
        Body: buffer,
        ContentType: 'application/octet-stream',
      };
      await s3.send(new PutObjectCommand(s3Params));
      console.log(`File ${fileName} successfully uploaded to S3.`);
    }

    // Registra el éxito en DynamoDB
    const dynamoParams = {
      TableName: DYNAMO_TABLE,
      Item: {
        fileId: { S: fileId },
        fileName: { S: fileName },
        migratedAt: { S: new Date().toISOString() },
        status: { S: 'SUCCESS' },
      },
    };
    await dynamoDB.send(new PutItemCommand(dynamoParams));
    console.log(`Migration of file ${fileName} logged in DynamoDB.`);
    answer = true;

    // Query ContentVersion and set S3_Migration__c = true
    try {
      const contentVersionResult = await conn.query(`
        SELECT Id, PathOnClient, FileType, ContentDocumentId, S3_Migration__c
        FROM ContentVersion
        WHERE ContentDocumentId = '${fileId}'
      `);

      // If the record(s) exist, update each to mark S3 migration as complete
      if (contentVersionResult && contentVersionResult.totalSize > 0) {
        for (const versionRec of contentVersionResult.records) {
          await conn.sobject('ContentVersion').update({
            Id: versionRec.Id,
            S3_Migration__c: true
          });
          console.log(`Set S3_Migration__c = true on ContentVersion ${versionRec.Id}`);
        }
      } else {
        console.log('No matching ContentVersion found to update.');
      }
    } catch (sfError) {
      //If updating S3_Migration__c fails, log to DynamoDB
      console.error(`Error updating S3_Migration__c for file ${fileName} in Salesforce:`, sfError);

      const dynamoErrorParams = {
        TableName: DYNAMO_TABLE,
        Item: {
          fileName: { S: fileName },
          fileId: { S: fileId },
          migratedAt: { S: new Date().toISOString() },
          status: { S: 'SF_UPDATE_FAILED' },
          errorMessage: { S: sfError.message },
        },
      };
      await dynamoDB.send(new PutItemCommand(dynamoErrorParams));
      console.log(`Salesforce update failure for file ${fileName} logged in DynamoDB.`);
    }
  } catch (uploadError) {
    // Handle errors from the upload step
    console.error(`Error uploading ${fileName} to S3:`, uploadError);
    logErrorToFile(`Error uploading file ${fileName} to S3: ${uploadError.message}`);

    const dynamoErrorParams = {
      TableName: DYNAMO_TABLE,
      Item: {
        fileName: { S: fileName },
        fileId: { S: fileId },
        migratedAt: { S: new Date().toISOString() },
        status: { S: 'FAILED' },
        errorMessage: { S: uploadError.message },
      },
    };
    await dynamoDB.send(new PutItemCommand(dynamoErrorParams));
    console.log(`Error for file ${fileName} logged in DynamoDB.`);
  } finally {
    return answer;
  }
}

// ────────────────────────────────────────────────────────────────────────────────
// fetch all Salesforce records in one paginated call
async function fetchAllRecords(filters) {
  const all = [];
  let result = await conn.query(buildQuery(filters));
  all.push(...result.records);
  while (!result.done) {
    result = await conn.queryMore(result.nextRecordsUrl);
    all.push(...result.records);
  }
  console.log(`Fetched ${all.length} Salesforce records.`);
  return all;
}

// list every S3 key under our folder prefix
async function listS3Keys() {
  const keys = [];
  let ContinuationToken;
  const prefix = S3_FOLDER.replace(/\/$/, '') + '/';
  do {
    const resp = await s3.send(new ListObjectsV2Command({
      Bucket: S3_BUCKET,
      Prefix: prefix,
      ContinuationToken
    }));
    if (resp.Contents) {
      for (const obj of resp.Contents) keys.push(obj.Key);
    }
    ContinuationToken = resp.IsTruncated ? resp.NextContinuationToken : null;
  } while (ContinuationToken);
  console.log(`Found ${keys.length} existing S3 objects.`);
  return new Set(keys);
}

// ────────────────────────────────────────────────────────────────────────────────
// REPLACED main day‐by‐day driver with a single‐shot + pause + migrate‐missing
(async () => {
  try {
    await conn.login(SF_USERNAME, SF_PASSWORD);
    console.log('✔ Salesforce login successful!');
  } catch (err) {
    console.error('❌ Salesforce login failed:', err.message);
    process.exit(1);
  }

  // 1) fetch everything, 2) fetch S3, 3) diff, 4) pause, 5) process only missing
  try {
    const sfRecords = await fetchAllRecords(filterDate);
    const s3Keys = await listS3Keys();
    const missing = sfRecords.filter(rec => {
      const parentId = rec.LinkedEntityId;
      const title = rec.ContentDocument.Title.replace(/\s+/g, '_');
      const extension = rec.ContentDocument.LatestPublishedVersion.FileExtension.toLowerCase();
      const key = `${S3_FOLDER}/${parentId}/${title}.${extension}`;
      return !s3Keys.has(key);
    });

    console.log(`\n🚨  Found ${missing.length} missing items between ${filterDate.startDate} and ${filterDate.endDate}.\n` +
      `Press any key to start migrating those…`);
    process.stdin.setRawMode(true);
    await new Promise(res => process.stdin.once('data', res));
    process.stdin.setRawMode(false);

    console.log('\nStarting migration of missing items…');
    let migratedCount = 0;
    for (const rec of missing) {
      const ok = await processRecord(rec);
      if (ok) migratedCount++;
    }
    console.log(`✔ ${migratedCount} files migrated.`);
    process.exit(0);

  } catch (err) {
    console.error('❌ Unexpected error during migration:', err);
    process.exit(1);
  }
})();