require('dotenv').config({ override: true });
const { S3Client, PutObjectCommand, CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand } = require('@aws-sdk/client-s3');
const { NodeHttpHandler } = require('@aws-sdk/node-http-handler');
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
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE, 10);  // Batch size for pagination
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
async function fileExistsInDynamoDB(fileId) {
  const params = {
    TableName: DYNAMO_TABLE,
    Key: {
      fileId: { S: fileId }
    }
  };

  try {
    const result = await dynamoDB.send(new GetItemCommand(params));
    return !!result.Item;  // If result.Item is truthy, the file exists
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

  let baseQuery = `SELECT Id, ContentDocument.CreatedDate ,ContentDocument.Title, ContentDocumentId, ContentDocument.FileType, ContentDocument.LatestPublishedVersion.VersionData 
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

  console.log("base query",baseQuery)

  return baseQuery;
}


// Function to run the SOQL query and handle pagination using queryMore
async function runQueryWithPagination(filters) {
  try {
    const query = buildQuery(filters);  // Build the query
    let result = await conn.query(query);  // Initial query call

    // Check if records are found
    if (result.records.length > 0) {
      console.log(`Found ${result.totalSize} records.`);

      // Loop to process records and paginate if necessary
      while (true) {
        console.log(`Processing ${result.records.length} records...`);

        // Process the current batch of records
        for (const record of result.records) {
          await processRecord(record);  // Your logic to process each record
        }

        // If there are more records to fetch, use queryMore
        if (!result.done) {
          console.log(`Fetching more records...`);
          result = await conn.queryMore(result.nextRecordsUrl);  // Fetch more records using pagination
        } else {
          console.log('All records processed successfully.');
          break;
        }
      }
    } else {
      console.log('No records found.');
    }
  } catch (err) {
    console.error('Error running query with pagination:', err);
  }
}

// Helper to delete the file from Salesforce
async function deleteFileFromSalesforce(contentDocumentId) {
  try {
    const deleteResult = await conn.sobject('ContentDocument').delete(contentDocumentId);

    if (Array.isArray(deleteResult)) {
      // If we got an array, check the first item
      if (!deleteResult[0].success) {
        throw new Error(
          `Salesforce delete failed: ${deleteResult[0].errors.join(', ')}`
        );
      }
    } else if (!deleteResult.success) {
      throw new Error(`Salesforce delete failed: ${deleteResult.errors.join(', ')}`);
    }

    console.log(`File [ContentDocumentId=${contentDocumentId}] successfully deleted from Salesforce.`);
    return true;
  } catch (err) {
    console.error(`Error deleting Salesforce ContentDocumentId=${contentDocumentId}:`, err);
    // We re-throw so we can handle it in processRecord
    throw err;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Example record processing function (download, upload, log, delete from SF)
async function processRecord(record) {
  const id = record.ContentDocumentId;
  const title = record.ContentDocument.Title.replace(/\s+/g, '_'); // Reemplaza espacios en el título por guiones bajos
  const fileExtension = record.ContentDocument.FileType;
  const fileName = `${S3_FOLDER}/${id}/${title}`;
  const createdDate = record.ContentDocument.CreatedDate;
  const fileId = id; // same as record.ContentDocumentId
  const versionDataUrl = record.ContentDocument.LatestPublishedVersion.VersionData;

  console.log("------------------------------------------------------");
  console.log(`Handling record -> File ID: ${id} | Created Date: ${createdDate} | File: ${fileName}`);

  // Check if file already exists in Dynamo
  const fileExists = await fileExistsInDynamoDB(fileId);
  if (fileExists) {
    console.log(`File ${fileName}.${fileExtension} already exists. Skipping upload.`);
    return;
  }

  // Download the file
  console.log(`Downloading file ID: ${id} - ${createdDate} - ${fileName}`);
  const buffer = await downloadFile(versionDataUrl);

  // If download failed (buffer is null), skip to the next recored and register the error
  if (!buffer) {
    logErrorToFile(`Failed to download file [${fileName}] from URL: ${versionDataUrl}`);
    console.log(`Skipping file ${fileName} due to download failure.`);
    throw new Error(`Failed to download: ${fileName}`);
  }

  // Upload to S3
  try {
    if (buffer.length > MULTIPART_THRESHOLD) {
      console.log(`File ${fileName} is larger than threshold, using Multipart Upload...`);
      await uploadLargeFileToS3(fileName, buffer);
    } else {
      console.log(`Uploading file ${fileName} to S3...`);
      const s3Params = {
        Bucket: S3_BUCKET,
        Key: fileName,
        Body: buffer,
        ContentType: 'application/octet-stream'
      };
      await s3.send(new PutObjectCommand(s3Params));
      console.log(`File ${fileName} successfully uploaded to S3.`);
    }

    // If we reach here, the upload was successful – log success in Dynamo
    const dynamoParams = {
      TableName: DYNAMO_TABLE,
      Item: {
        fileId: { S: fileId },
        fileName: { S: fileName },
        migratedAt: { S: new Date().toISOString() },
        status: { S: 'SUCCESS' }
      }
    };
    await dynamoDB.send(new PutItemCommand(dynamoParams));
    console.log(`Migration of file ${fileName} logged in DynamoDB.`);

    // ─────────────────────────────────────────────────────────────────────────
    // Delete from Salesforce now that we know it's safe
    try {
      await deleteFileFromSalesforce(id);
    } catch (deleteError) {
      // If Salesforce delete fails, log to Dynamo so we know it wasn’t removed
      console.error(`Could not delete file ${fileName} from SF:`, deleteError);
      logErrorToFile(`Could not delete file [${fileName}] in SF: ${deleteError.message}`);

      const dynamoDeleteErrorParams = {
        TableName: DYNAMO_TABLE,
        Item: {
          fileId: { S: fileId },
          fileName: { S: fileName },
          migratedAt: { S: new Date().toISOString() },
          status: { S: 'DELETE_FAILED' },
          errorMessage: { S: deleteError.message }
        }
      };
      await dynamoDB.send(new PutItemCommand(dynamoDeleteErrorParams));
    }
    // ─────────────────────────────────────────────────────────────────────────
  } catch (uploadError) {
    console.error(`Error uploading ${fileName} to S3:`, uploadError);
    logErrorToFile(`Error uploading file ${fileName} to S3: ${uploadError.message}`);

    // Log the failure in DynamoDB
    const dynamoErrorParams = {
      TableName: DYNAMO_TABLE,
      Item: {
        fileId: { S: fileId },
        fileName: { S: fileName },
        migratedAt: { S: new Date().toISOString() },
        status: { S: 'FAILED' },
        errorMessage: { S: uploadError.message }
      }
    };
    await dynamoDB.send(new PutItemCommand(dynamoErrorParams));
    console.log(`Error for file ${fileName} logged in DynamoDB.`);
  }
}

// Function to start migration with filters
async function migrateDataWithFilters(filters) {
  try {
    // Log in to Salesforce
    await conn.login(SF_USERNAME, SF_PASSWORD);
    console.log('Successfully connected to Salesforce.');

    // Run query with pagination and filters
    await runQueryWithPagination(filters);

    console.log('Migration process completed.');
  } catch (err) {
    console.error('Error in migration process:', err);
  }
}

// Example: Add filters to the query
const filters = [
  { field: 'ContentDocument.FileType', operator: '=', value: 'bin' },               // Filtrar por tipo de archivo
  { field: 'LinkedEntityId', operator: 'IN', value: '(SELECT Id FROM Opportunity)' } // Filtrar por ID de Opportunity
];

// Función para añadir un día a una fecha
function addDays(date, days) {
  const result = new Date(date);
  result.setDate(result.getDate() + days);
  return result;
}

// Función para formatear la fecha en el formato 'YYYY-MM-DDTHH:mm:ssZ'
function formatDateToISOString(date) {
  return date.toISOString().split('T')[0] + 'T00:00:00Z';
}

async function migrateDataByDay(startDate, endDate) {
  let currentDate = new Date(startDate);
  const finalDate = new Date(endDate);

  // Itera desde la fecha inicial hasta la fecha final
  while (currentDate <= finalDate) {
    // Define el rango de fecha para el día actual
    const nextDate = addDays(currentDate, 1);
    const fitersDates = {
      startDate: formatDateToISOString(currentDate),  // Día actual
      endDate: formatDateToISOString(nextDate),       // Día siguiente
    };

    console.log(`Running migration for date: ${fitersDates.startDate} to ${fitersDates.endDate}`);

    try {
      // Ejecuta la migración para el día actual
      await migrateDataWithFilters(fitersDates);
    } catch (error) {
      console.error(`Error during migration for ${fitersDates.startDate}:`, error);
    }

    // Incrementa la fecha actual al siguiente día
    currentDate = nextDate;
  }

  console.log('All migrations completed.');
}


// Execute the migration with filters
//migrateDataWithFilters(filters);
migrateDataByDay(filterDate.startDate, filterDate.endDate);
