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


// Example record processing function (download and upload)
async function processRecord(record) {
  const id = record.ContentDocumentId;
  const title = record.ContentDocument.Title.replace(/\s+/g, '_'); // Reemplaza espacios en el título por guiones bajos
  const fileExtension = record.ContentDocument.FileType;
  const fileName = `${S3_FOLDER}/${id}/${title}`;
  const createdDate = record.ContentDocument.CreatedDate;
  const fileId = id;
  const versionDataUrl = record.ContentDocument.LatestPublishedVersion.VersionData;

  console.log("------------------------------------------------------")
  // Verifica si el archivo ya existe en DynamoDB
  const fileExists = await fileExistsInDynamoDB(fileId);
  if (fileExists) {
    console.log(`File ${fileName}.${fileExtension} already exists. Skipping upload.`);
    return;
  }

  // Descarga el archivo
  console.log(`Downloading file ID: ${id} - ${createdDate} - ${fileName}`);
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

    // Query ContentVersion and set S3_Migration__c = true
    try {
      const contentVersionResult = await conn.query(`
        SELECT Id, PathOnClient, FileType, ContentDocumentId, S3_Migration__c
        FROM ContentVersion
        WHERE ContentDocumentId = '${id}'
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
          fileId: { S: fileId },
          fileName: { S: fileName },
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
        fileId: { S: fileId },
        fileName: { S: fileName },
        migratedAt: { S: new Date().toISOString() },
        status: { S: 'FAILED' },
        errorMessage: { S: uploadError.message },
      },
    };
    await dynamoDB.send(new PutItemCommand(dynamoErrorParams));
    console.log(`Error for file ${fileName} logged in DynamoDB.`);
  }
}


// Function to start migration with filters
async function migrateDataWithFilters(filters) {
  try {
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
    const filterDate = {
      startDate: formatDateToISOString(currentDate),  // Día actual
      endDate: formatDateToISOString(nextDate),       // Día siguiente
    };

    console.log(`Running migration for date: ${filterDate.startDate} to ${filterDate.endDate}`);

    try {
      // Ejecuta la migración para el día actual
      await migrateDataWithFilters(filterDate);
    } catch (error) {
      console.error(`Error during migration for ${filterDate.startDate}:`, error);
    }

    // Incrementa la fecha actual al siguiente día
    currentDate = nextDate;
  }

  console.log('All migrations completed.');
}

(async () => {
  // 1) do ONE login, then exit on failure
  try {
    await conn.login(SF_USERNAME, SF_PASSWORD);
    console.log('✔ Salesforce login successful!');
  } catch (err) {
    console.error('❌ Salesforce login failed:', err.message);
    process.exit(1);
  }

  // 2) if login worked, run your day‐by‐day migration
  try {
    await migrateDataByDay(filterDate.startDate, filterDate.endDate);
    console.log('✔ All migrations completed.');
    process.exit(0);
  } catch (err) {
    console.error('❌ Unexpected error during migration:', err);
    process.exit(1);
  }
})();