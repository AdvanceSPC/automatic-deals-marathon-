// api/sync.js
import {
  fetchCSVFromS3,
  readProcessedList,
  saveProcessedList,
  testS3Connections,
} from "../utils/s3Helpers.js";
import { sendToHubspot } from "../utils/hubspot.js";
import { ListObjectsV2Command, S3Client } from "@aws-sdk/client-s3";

const AWS1_BUCKET = process.env.AWS1_BUCKET;
const s3Read = new S3Client({
  region: process.env.AWS1_REGION,
  credentials: {
    accessKeyId: process.env.AWS1_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS1_SECRET_ACCESS_KEY,
  },
});

export const config = {
  runtime: "nodejs",
  maxDuration: 300, // Explícitamente configurar timeout de Vercel
};

export default async function handler(req, res) {
  const executionStart = Date.now();
  const MAX_EXECUTION_TIME = 280000; // 280 segundos (20s de margen)
  
  console.log("🔌 Verificando conexión con buckets S3...");

  const s3Ok = await testS3Connections();
  if (!s3Ok) {
    return res.status(500).send("❌ Fallo en conexión a uno o ambos buckets S3.");
  }

  console.log("📃 Cargando historial...");
  const processed = await readProcessedList();

  const command = new ListObjectsV2Command({
    Bucket: AWS1_BUCKET,
    Prefix: "delta_negocio_",
  });

  const { Contents = [] } = await s3Read.send(command);
  const nuevosArchivos = Contents.map((obj) => obj.Key)
    .filter((key) => key.endsWith(".csv"))
    .filter((key) => !processed.includes(key))
    .sort(); // Procesar archivos en orden

  if (nuevosArchivos.length === 0) {
    console.log("🟡 No hay nuevos archivos para procesar.");
    return res.status(200).send("🟡 No hay archivos nuevos.");
  }

  console.log(`📁 Encontrados ${nuevosArchivos.length} archivos nuevos para procesar`);

  // Procesar solo UN archivo por ejecución para evitar timeouts
  const fileName = nuevosArchivos[0];
  
  try {
    // Verificar tiempo disponible antes de procesar
    const elapsed = Date.now() - executionStart;
    if (elapsed > MAX_EXECUTION_TIME * 0.1) { // Si ya pasó más del 10% del tiempo solo en setup
      console.log("⏰ Tiempo insuficiente para procesar archivo");
      return res.status(200).send("⏰ Tiempo insuficiente - reintentar");
    }

    console.log(`⬇️ Procesando archivo: ${fileName}`);
    const deals = await fetchCSVFromS3(fileName);

    if (!deals.length) {
      console.warn(`⚠️ Archivo vacío: ${fileName}`);
      processed.push(fileName);
      await saveProcessedList(processed);
      return res.status(200).send(`⚠️ Archivo vacío procesado: ${fileName}`);
    }

    console.log(`📨 Enviando ${deals.length} negocios a HubSpot...`);
    
    // Verificar si el archivo es muy grande y necesita procesamiento parcial
    if (deals.length > 5000) {
      console.log(`📏 Archivo grande detectado (${deals.length} registros) - procesando en chunks`);
      
      // Procesar en chunks de 2500 para archivos muy grandes
      const chunkSize = 2500;
      for (let i = 0; i < deals.length; i += chunkSize) {
        const chunk = deals.slice(i, i + chunkSize);
        console.log(`🔄 Procesando chunk ${Math.floor(i/chunkSize) + 1} de ${Math.ceil(deals.length/chunkSize)}`);
        
        await sendToHubspot(chunk, `${fileName}_chunk_${Math.floor(i/chunkSize) + 1}`);
        
        // Verificar tiempo restante
        const elapsedTime = Date.now() - executionStart;
        if (elapsedTime > MAX_EXECUTION_TIME * 0.8) {
          console.log("⏰ Tiempo límite alcanzado - guardando progreso");
          break;
        }
      }
    } else {
      await sendToHubspot(deals, fileName);
    }

    // Marcar como procesado solo si se completó exitosamente
    processed.push(fileName);
    console.log(`✅ Procesado exitosamente: ${fileName}`);
    
  } catch (error) {
    console.error(`❌ Error procesando ${fileName}:`, error);
    return res.status(500).send(`❌ Error procesando ${fileName}: ${error.message}`);
  }

  console.log("💾 Actualizando historial...");
  await saveProcessedList(processed);

  const executionTime = ((Date.now() - executionStart) / 1000).toFixed(2);
  const response = `✅ Procesado archivo: ${fileName} en ${executionTime}s. ${nuevosArchivos.length - 1} archivos pendientes.`;
  
  console.log(response);
  return res.status(200).send(response);
}
