// utils/s3Helpers.js
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  ListObjectsV2Command,
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
    await s3Read.send(new ListObjectsV2Command({ Bucket: process.env.AWS1_BUCKET, MaxKeys: 1 }));
    await s3Hist.send(new ListObjectsV2Command({ Bucket: process.env.AWS2_BUCKET, MaxKeys: 1 }));
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

for (const fileName of nuevosArchivos) {
  try {
    process.env.CURRENT_FILENAME = fileName;
    console.log(`⬇️ Procesando archivo: ${fileName}`);
    const deals = await fetchCSVFromS3(fileName);

    if (!deals.length) {
      console.warn(`⚠️ Archivo vacío: ${fileName}`);
      continue;
    }

    console.log(`📨 Enviando ${deals.length} negocios a HubSpot...`);
    await sendToHubspot(deals);

    processed.push(fileName);
    console.log(`✅ Procesado exitosamente: ${fileName}`);
  } catch (error) {
    console.error(`❌ Error procesando ${fileName}:`, error);
  }
}
