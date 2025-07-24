// utils/s3Helpers.js
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  ListObjectsV2Command,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import csv from "csv-parser";
import { Readable } from "stream";

// Cliente S3 cuenta Marathon (lectura del CSV)
const s3Read = new S3Client({
  region: process.env.AWS1_REGION,
  credentials: {
    accessKeyId: process.env.AWS1_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS1_SECRET_ACCESS_KEY,
  },
});

// Cliente S3 cuenta Advance (guardar historial)
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
    Key: `reportes/${fileName}`,
    Body: content,
    ContentType: "text/plain",
  });
  await s3Hist.send(command);
  console.log(`📝 Reporte guardado como: reportes/${fileName}`);
}

// Nueva función para guardar progreso parcial
export async function savePartialProgress(fileName, processedCount, totalCount) {
  const progressKey = `progress/${fileName.replace('.csv', '')}_progress.json`;
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

// Nueva función para manejar archivos parciales
export async function savePartialFileProgress(fileName, processedCount, totalCount) {
  const partialKey = `partial/${fileName.replace('.csv', '')}_partial.json`;
  const partialData = {
    fileName,
    processedRecords: processedCount,
    totalRecords: totalCount,
    nextStartIndex: processedCount,
    timestamp: new Date().toISOString(),
    status: 'partial'
  };
  
  const command = new PutObjectCommand({
    Bucket: process.env.AWS2_BUCKET,
    Key: partialKey,
    Body: JSON.stringify(partialData, null, 2),
    ContentType: "application/json",
  });
  
  await s3Hist.send(command);
  console.log(`💾 Progreso parcial guardado: ${processedCount}/${totalCount} registros`);
}

// Función para obtener archivos parciales pendientes
export async function getPartialFiles() {
  try {
    const command = new ListObjectsV2Command({
      Bucket: process.env.AWS2_BUCKET,
      Prefix: "partial/",
    });
    
    const response = await s3Hist.send(command);
    const partialFiles = [];
    
    for (const obj of response.Contents || []) {
      try {
        const getCommand = new GetObjectCommand({
          Bucket: process.env.AWS2_BUCKET,
          Key: obj.Key,
        });
        const data = await s3Hist.send(getCommand);
        const content = await data.Body.transformToString();
        const partialInfo = JSON.parse(content);
        partialFiles.push(partialInfo);
      } catch (err) {
        console.error(`Error leyendo archivo parcial ${obj.Key}:`, err);
      }
    }
    
    return partialFiles;
  } catch (err) {
    console.error("Error obteniendo archivos parciales:", err);
    return [];
  }
}

// Función para eliminar registro de archivo parcial (cuando se completa)
export async function removePartialFile(fileName) {
  try {
    const partialKey = `partial/${fileName.replace('.csv', '')}_partial.json`;
    const command = new DeleteObjectCommand({
      Bucket: process.env.AWS2_BUCKET,
      Key: partialKey,
    });
    await s3Hist.send(command);
    console.log(`🗑️ Eliminado registro parcial: ${fileName}`);
  } catch (err) {
    console.error(`Error eliminando archivo parcial ${fileName}:`, err);
  }
}
