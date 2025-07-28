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
  maxDuration: 300, 
};

export default async function handler(req, res) {
  const executionStart = Date.now();
  // Aumentar el tiempo máximo a 290 segundos (290000ms)
  const MAX_EXECUTION_TIME = 290000;
  
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
    .sort(); 

  if (nuevosArchivos.length === 0) {
    console.log("🟡 No hay nuevos archivos para procesar.");
    return res.status(200).send("🟡 No hay archivos nuevos.");
  }

  console.log(`📁 Encontrados ${nuevosArchivos.length} archivos nuevos para procesar`);

  // Procesar solo 1 archivo para evitar timeouts
  const fileName = nuevosArchivos[0];
  
  try {
    const elapsed = Date.now() - executionStart;
    // Reducir el check inicial a 5% del tiempo total
    if (elapsed > MAX_EXECUTION_TIME * 0.05) { 
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
      
      const chunkSize = 2500;
      let processedChunks = 0;
      let totalProcessed = 0;
      
      for (let i = 0; i < deals.length; i += chunkSize) {
        const chunk = deals.slice(i, i + chunkSize);
        const chunkNumber = Math.floor(i/chunkSize) + 1;
        const totalChunks = Math.ceil(deals.length/chunkSize);
        
        console.log(`🔄 Procesando chunk ${chunkNumber} de ${totalChunks}`);
        
        // Calcular tiempo restante para este chunk
        const elapsedTime = Date.now() - executionStart;
        const remainingTime = MAX_EXECUTION_TIME - elapsedTime;
        
        // Necesitamos al menos 30 segundos para procesar un chunk
        if (remainingTime < 30000) {
          console.log(`⏰ Tiempo insuficiente para chunk ${chunkNumber} (${Math.round(remainingTime/1000)}s restantes)`);
          break;
        }
        
        const result = await sendToHubspot(chunk, `${fileName}_chunk_${chunkNumber}`, remainingTime);
        processedChunks++;
        totalProcessed += chunk.length;
        
        // Verificar tiempo después de cada chunk
        const newElapsedTime = Date.now() - executionStart;
        if (newElapsedTime > MAX_EXECUTION_TIME * 0.85) {
          console.log(`⏰ Límite de tiempo alcanzado después del chunk ${chunkNumber}`);
          console.log(`📊 Procesados ${processedChunks}/${totalChunks} chunks (${totalProcessed}/${deals.length} registros)`);
          break;
        }
      }
      
      // Solo marcar como completamente procesado si se procesaron todos los chunks
      if (processedChunks === Math.ceil(deals.length/chunkSize)) {
        processed.push(fileName);
        console.log(`✅ Procesado exitosamente: ${fileName} (todos los chunks completados)`);
      } else {
        console.log(`⚠️ Procesamiento parcial: ${fileName} (${processedChunks}/${Math.ceil(deals.length/chunkSize)} chunks)`);
      }
      
    } else {
      const remainingTime = MAX_EXECUTION_TIME - (Date.now() - executionStart);
      await sendToHubspot(deals, fileName, remainingTime);
      processed.push(fileName);
      console.log(`✅ Procesado exitosamente: ${fileName}`);
    }
    
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
