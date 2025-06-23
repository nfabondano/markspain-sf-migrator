require('dotenv').config({ override: true });
const {
  S3Client,
  PutObjectCommand,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  HeadObjectCommand
} = require('@aws-sdk/client-s3'); const { NodeHttpHandler } = require('@aws-sdk/node-http-handler');
const { DynamoDBClient, PutItemCommand, ScanCommand } = require('@aws-sdk/client-dynamodb');
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
const S3_FOLDERS = process.env.S3_FOLDER.split(',');
const DYNAMO_TABLE = process.env.DYNAMO_TABLE;
const MULTIPART_THRESHOLD = parseInt(process.env.MULTIPART_THRESHOLD, 10);

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


function buildQuery(filters, folder) {
  console.log("FILTERS")
  console.log(filters)

  let baseQuery = `SELECT Id, LinkedEntityId, ContentDocumentId, ContentDocument.CreatedDate, ContentDocument.LatestPublishedVersionId, ContentDocument.LatestPublishedVersion.PathOnClient, ContentDocument.LatestPublishedVersion.VersionData 
  FROM ContentDocumentLink 
  WHERE LinkedEntityId IN (SELECT Id FROM ${folder}) 
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
async function processRecord(record, folder) {
  let answer = false;

  const fileId = record.ContentDocumentId;
  const parentId = record.LinkedEntityId;
  const pathOnClient = record.ContentDocument.LatestPublishedVersion.PathOnClient;

  const baseName = path.basename(pathOnClient).replace(/\s+/g, '_');
  const parentPrefix = `${folder}/${parentId}/`;
  let s3Key = `${parentPrefix}${baseName}`;

  try {
    // If this succeeds the name is already taken → keep the id-scoped folder
    await s3.send(new HeadObjectCommand({ Bucket: S3_BUCKET, Key: s3Key }));
    s3Key = `${parentPrefix}${fileId}_${baseName}/${baseName}`;
    console.log(`Duplicate detected – will store in ${s3Key}`);
  } catch (e) {
    // 404/NotFound is expected when the object doesn’t exist – ignore
    if (e.$metadata?.httpStatusCode !== 404 && e.name !== 'NotFound') throw e;
  }

  // what we log to Dynamo never changes
  const dynamoFileName = `${parentPrefix}${fileId}_${baseName}/${baseName}`;
  const createdDate = record.ContentDocument.CreatedDate;
  const versionDataUrl = record.ContentDocument.LatestPublishedVersion.VersionData;

  console.log("------------------------------------------------------")
  // DynamoDB already checked up-front; no need to re-check here

  // Descarga el archivo
  console.log(`Downloading file ID: ${fileId} - ${createdDate} - ${s3Key}`);
  const buffer = await downloadFile(versionDataUrl);

  // Si la descarga falló (buffer es null), salta al siguiente registro y registra el error
  if (!buffer) {
    logErrorToFile(`Failed to download file ${s3Key} with URL: ${versionDataUrl}`);
    console.log(`Skipping file ${s3Key} due to download failure.`);
    throw new Error(`Failed to download: ${s3Key}`);
  }

  // Carga el archivo en S3
  try {
    if (buffer.length > MULTIPART_THRESHOLD) {
      console.log(`File ${s3Key} is larger than 5MB, using Multipart Upload...`);
      await uploadLargeFileToS3(s3Key, buffer);
    } else {
      console.log(`Uploading file ${s3Key} to S3...`);
      const s3Params = {
        Bucket: S3_BUCKET,
        Key: s3Key, // Usa la ruta con Opportunity/id para simular la estructura de carpetas
        Body: buffer,
        ContentType: 'application/octet-stream',
      };
      await s3.send(new PutObjectCommand(s3Params));
      console.log(`File ${s3Key} successfully uploaded to S3.`);
    }

    // Registra el éxito en DynamoDB
    const dynamoParams = {
      TableName: DYNAMO_TABLE,
      Item: {
        fileId: { S: fileId },
        fileName: { S: dynamoFileName },
        migratedAt: { S: new Date().toISOString() },
        status: { S: 'SUCCESS' },
      },
    };
    await dynamoDB.send(new PutItemCommand(dynamoParams));
    console.log(`Migration of file ${dynamoFileName} logged in DynamoDB.`);
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
            Id: record.ContentDocument.LatestPublishedVersionId,
            S3_Migration__c: true
          });
          console.log(`Set S3_Migration__c = true on ContentVersion ${versionRec.Id}`);
        }
      } else {
        console.log('No matching ContentVersion found to update.');
      }
    } catch (sfError) {
      //If updating S3_Migration__c fails, log to DynamoDB
      console.error(`Error updating S3_Migration__c for file ${dynamoFileName} in Salesforce:`, sfError);

      const dynamoErrorParams = {
        TableName: DYNAMO_TABLE,
        Item: {
          fileName: { S: dynamoFileName },
          fileId: { S: fileId },
          migratedAt: { S: new Date().toISOString() },
          status: { S: 'SF_UPDATE_FAILED' },
          errorMessage: { S: sfError.message },
        },
      };
      await dynamoDB.send(new PutItemCommand(dynamoErrorParams));
      console.log(`Salesforce update failure for file ${dynamoFileName} logged in DynamoDB.`);
    }
  } catch (uploadError) {
    // Handle errors from the upload step
    console.error(`Error uploading ${s3Key} to S3:`, uploadError);
    logErrorToFile(`Error uploading file ${s3Key} to S3: ${uploadError.message}`);

    const dynamoErrorParams = {
      TableName: DYNAMO_TABLE,
      Item: {
        fileName: { S: dynamoFileName },
        fileId: { S: fileId },
        migratedAt: { S: new Date().toISOString() },
        status: { S: 'FAILED' },
        errorMessage: { S: uploadError.message },
      },
    };
    await dynamoDB.send(new PutItemCommand(dynamoErrorParams));
    console.log(`Error for file ${dynamoFileName} logged in DynamoDB.`);
  } finally {
    return answer;
  }
}

// ────────────────────────────────────────────────────────────────────────────────
// fetch all Salesforce records in one paginated call
async function fetchAllRecords(filters, folder) {
  const all = [];
  let result = await conn.query(buildQuery(filters, folder));
  all.push(...result.records);
  while (!result.done) {
    result = await conn.queryMore(result.nextRecordsUrl);
    all.push(...result.records);
  }
  console.log(`Fetched ${all.length} Salesforce records.`);
  return all;
}

// list already-migrated keys from DynamoDB instead of S3
async function listDynamoKeys(folder) {
  const keys = new Set();
  let ExclusiveStartKey;
  const prefix = folder.replace(/\/$/, '') + '/';
  do {
    const resp = await dynamoDB.send(new ScanCommand({
      TableName: DYNAMO_TABLE,
      ProjectionExpression: 'fileName',
      ExclusiveStartKey
    }));
    if (resp.Items) {
      for (const i of resp.Items) {
        const name = i.fileName?.S;
        if (name?.startsWith(prefix)) keys.add(name);
      }
    }
    ExclusiveStartKey = resp.LastEvaluatedKey;
  } while (ExclusiveStartKey);
  console.log(`Found ${keys.size} existing DynamoDB records.`);
  return keys;
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
    for (const folder of S3_FOLDERS) {
      // pre-load your already-migrated keys once per folder
      const migratedKeys = await listDynamoKeys(folder);

      // compute first and last year
      const startYear = new Date(filterDate.startDate).getFullYear();
      const endYear = new Date(filterDate.endDate).getFullYear();

      // loop one calendar-year at a time
      for (let year = startYear; year <= endYear; year++) {
        // build a per-year date range
        const yearStart = `${year}-01-01T00:00:00Z`;
        const yearEnd = `${year}-12-31T23:59:59Z`;
        console.log(`\n➡️  Processing ${folder}, Year ${year} (${yearStart} → ${yearEnd})`);

        // fetch only that year’s records
        const sfRecords = await fetchAllRecords(
          { startDate: yearStart, endDate: yearEnd },
          folder
        );

        // find only the ones we haven’t migrated yet
        const missing = sfRecords.filter(rec => {
          const baseName = path.basename(rec.ContentDocument.LatestPublishedVersion.PathOnClient).replace(/\s+/g, '_');
          const key = `${folder}/${rec.LinkedEntityId}/${rec.ContentDocumentId}_${baseName}/${baseName}`;
          return !migratedKeys.has(key);
        });
        console.log(`→ Found ${missing.length} missing files in ${year}`);

        // migrate that slice
        let migratedCount = 0;
        for (const rec of missing) {
          if (await processRecord(rec, folder)) migratedCount++;
        }
        console.log(`✔ Migrated ${migratedCount}/${missing.length} for ${year}`);
      }
    }
    process.exit(0);

  } catch (err) {
    console.error('❌ Unexpected error during migration:', err);
    process.exit(1);
  }
})();