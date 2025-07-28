//Negocios
// utils/s3Helpers.js
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  ListObjectsV2Command,
} from "@aws-sdk/client-s3";
import csv from "csv-parser";
import { Readable } from "stream";

// S3 cuenta Marathon (lectura del CSV)
const s3Read = new S3Client({
  region: process.env.AWS1_REGION,
  credentials: {
    accessKeyId: process.env.AWS1_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS1_SECRET_ACCESS_KEY,
  },
});

// S3 cuenta Advance (guardar historial)
const s3Hist = new S3Client({
  region: process.env.AWS2_REGION,
  credentials: {
    accessKeyId: process.env.AWS2_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS2_SECRET_ACCESS_KEY,
  },
});

export async function fetchCSVFromS3(fileName) {
  const command = new GetObjectCommand({
    Bucket: process.env.AWS1_BUCKET,
    Key: fileName,
  });

  const data = await s3Read.send(command);
  const stream = Readable.from(data.Body);
  const deals = [];

  await new Promise((resolve, reject) => {
    stream
      .pipe(csv({ separator: ";" }))
      .on("data", (row) => {
        if (!row.contact_id) return;
        deals.push({
          properties: {
            dealname: row.linea || null,
            concepto: row.concepto || null,
            region: row.region || null,
            microsite_calculado: row.microsite_calculado || null,
            provincia_homologada: row.provincia_homologada || null,
            ciudad_centro: row.ciudad_centro || null,
            centro: row.centro || null,
            closedate: row.closedate || null,
            grupo: row.grupo || null,
            marca: row.marca || null,
            equipo: row.equipo || null,
            genero_edad: row.genero_edad || null,
            agrupador_categoria: row.agrupador_categoria || null,
            actividad: row.actividad || null,
            talla__codigo_: row.talla__codigo_ || null,
            nombre_campana: row.nombre_campana || null,
            amount: row.amount || null,
            dealstage: row.dealstage || null,
            pipeline: row.pipeline || null,
          },
          associations: [
            {
              types: [
                {
                  associationCategory: "HUBSPOT_DEFINED",
                  associationTypeId: 3,
                },
              ],
              to: {
                id: row.contact_id,
                type: "contact",
              },
            },
          ],
        });
      })
      .on("end", resolve)
      .on("error", reject);
  });

  return deals;
}

export async function readProcessedList() {
  try {
    const command = new GetObjectCommand({
      Bucket: process.env.AWS2_BUCKET,
      Key: process.env.PROCESSED_KEY,
    });
    const response = await s3Hist.send(command);
    const stream = await response.Body.transformToString();
    return JSON.parse(stream);
  } catch {
    return [];
  }
}

export async function saveProcessedList(list) {
  const command = new PutObjectCommand({
    Bucket: process.env.AWS2_BUCKET,
    Key: process.env.PROCESSED_KEY,
    Body: JSON.stringify(list, null, 2),
    ContentType: "application/json",
  });
  await s3Hist.send(command);
}

export async function testS3Connections() {
  try {
    await Promise.all([
      s3Read.send(new ListObjectsV2Command({ Bucket: process.env.AWS1_BUCKET, MaxKeys: 1 })),
      s3Hist.send(new ListObjectsV2Command({ Bucket: process.env.AWS2_BUCKET, MaxKeys: 1 }))
    ]);
    return true;
  } catch (err) {
    console.error("❌ Fallo en conexión a uno o ambos buckets S3:", err);
    return false;
  }
}

export async function saveReportToS3(content, fileName) {
  const command = new PutObjectCommand({
    Bucket: process.env.AWS2_BUCKET,
    Key: `reportes_negocios/${fileName}`,
    Body: content,
    ContentType: "text/plain",
  });
  await s3Hist.send(command);
  console.log(`📝 Reporte guardado como: reportes_negocios/${fileName}`);
}

// guardar progreso parcial
export async function savePartialProgress(fileName, processedCount, totalCount) {
  const progressKey = `progress_negocios/${fileName.replace('.csv', '')}_progress.json`;
  const progressData = {
    fileName,
    processedCount,
    totalCount,
    timestamp: new Date().toISOString(),
    status: processedCount >= totalCount ? 'completed' : 'processing'
  };
  
  const command = new PutObjectCommand({
    Bucket: process.env.AWS2_BUCKET,
    Key: progressKey,
    Body: JSON.stringify(progressData, null, 2),
    ContentType: "application/json",
  });
  
  await s3Hist.send(command);
}

export async function getFileProgress(fileName) {
  try {
    const progressKey = `file_progress_negocios/${fileName.replace('.csv', '')}_file_progress.json`;
    const command = new GetObjectCommand({
      Bucket: process.env.AWS2_BUCKET,
      Key: progressKey,
    });
    const response = await s3Hist.send(command);
    const stream = await response.Body.transformToString();
    return JSON.parse(stream);
  } catch {
    return null;
  }
}

export async function saveFileProgress(fileName, progressData) {
  const progressKey = `file_progress_negocios/${fileName.replace('.csv', '')}_file_progress.json`;
  const fullProgressData = {
    fileName,
    ...progressData,
    lastUpdated: new Date().toISOString()
  };
  
  const command = new PutObjectCommand({
    Bucket: process.env.AWS2_BUCKET,
    Key: progressKey,
    Body: JSON.stringify(fullProgressData, null, 2),
    ContentType: "application/json",
  });
  
  await s3Hist.send(command);
  console.log(`💾 Progreso actualizado: ${progressData.processedRecords}/${progressData.totalRecords} registros`);
}

export async function markChunkAsCompleted(fileName, chunkNumber, recordsCount) {
  const chunkKey = `chunks_negocios/${fileName.replace('.csv', '')}_chunk_${chunkNumber}.json`;
  const chunkData = {
    fileName,
    chunkNumber,
    recordsCount,
    status: 'completed',
    completedAt: new Date().toISOString()
  };
  
  const command = new PutObjectCommand({
    Bucket: process.env.AWS2_BUCKET,
    Key: chunkKey,
    Body: JSON.stringify(chunkData, null, 2),
    ContentType: "application/json",
  });
  
  await s3Hist.send(command);
}

export async function getCompletedChunks(fileName) {
  try {
    const prefix = `chunks_negocios${fileName.replace('.csv', '')}_chunk_`;
    const command = new ListObjectsV2Command({
      Bucket: process.env.AWS2_BUCKET,
      Prefix: prefix,
    });
    
    const response = await s3Hist.send(command);
    const completedChunks = [];
    
    if (response.Contents) {
      for (const object of response.Contents) {
        try {
          const chunkCommand = new GetObjectCommand({
            Bucket: process.env.AWS2_BUCKET,
            Key: object.Key,
          });
          const chunkResponse = await s3Hist.send(chunkCommand);
          const chunkData = JSON.parse(await chunkResponse.Body.transformToString());
          completedChunks.push(chunkData);
        } catch (error) {
          console.warn(`⚠️ Error leyendo chunk ${object.Key}:`, error);
        }
      }
    }
    
    return completedChunks.sort((a, b) => a.chunkNumber - b.chunkNumber);
  } catch {
    return [];
  }
}

export async function cleanupFileProgress(fileName) {
  const filesToCleanup = [
    `file_progress_negocios/${fileName.replace('.csv', '')}_file_progress.json`,
    `progress_negocios/${fileName.replace('.csv', '')}_progress.json`
  ];
  
  for (const key of filesToCleanup) {
    try {
      const command = new PutObjectCommand({
        Bucket: process.env.AWS2_BUCKET,
        Key: key,
        Body: JSON.stringify({ status: 'archived', completedAt: new Date().toISOString() }),
        ContentType: "application/json",
      });
      await s3Hist.send(command);
    } catch (error) {
      console.warn(`⚠️ Error limpiando ${key}:`, error);
    }
  }
}
