// api/sync.js
import {
  fetchCSVFromS3,
  readProcessedList,
  saveProcessedList,
  testS3Connections,
  getFileProgress,
  saveFileProgress,
  markChunkAsCompleted
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
    if (elapsed > MAX_EXECUTION_TIME * 0.05) { 
      console.log("⏰ Tiempo insuficiente para procesar archivo");
      return res.status(200).send("⏰ Tiempo insuficiente - reintentar");
    }

    console.log(`⬇️ Procesando archivo: ${fileName}`);
    
    // Verificar si el archivo ya tiene progreso parcial
    const fileProgress = await getFileProgress(fileName);
    let deals;
    
    if (fileProgress && fileProgress.totalRecords) {
      console.log(`🔄 Continuando procesamiento de ${fileName} desde chunk ${fileProgress.lastCompletedChunk + 1}`);
      console.log(`📊 Progreso anterior: ${fileProgress.processedRecords}/${fileProgress.totalRecords} registros`);
    } else {
      console.log(`🆕 Iniciando nuevo procesamiento de ${fileName}`);
    }
    
    deals = await fetchCSVFromS3(fileName);

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
      const totalChunks = Math.ceil(deals.length / chunkSize);
      let processedChunks = fileProgress ? fileProgress.lastCompletedChunk : 0;
      let totalProcessed = fileProgress ? fileProgress.processedRecords : 0;
      
      // Guardar el total de registros si es la primera vez
      if (!fileProgress || !fileProgress.totalRecords) {
        await saveFileProgress(fileName, {
          totalRecords: deals.length,
          totalChunks: totalChunks,
          processedRecords: 0,
          lastCompletedChunk: 0,
          status: 'processing'
        });
      }
      
      // Comenzar desde el siguiente chunk no completado
      const startChunk = processedChunks;
      
      for (let i = startChunk * chunkSize; i < deals.length; i += chunkSize) {
        const chunk = deals.slice(i, i + chunkSize);
        const chunkNumber = Math.floor(i/chunkSize) + 1;
        
        console.log(`🔄 Procesando chunk ${chunkNumber} de ${totalChunks}`);
        
        // Calcular tiempo restante para este chunk
        const elapsedTime = Date.now() - executionStart;
        const remainingTime = MAX_EXECUTION_TIME - elapsedTime;
        
        // Necesitamos al menos 30 segundos para procesar un chunk
        if (remainingTime < 30000) {
          console.log(`⏰ Tiempo insuficiente para chunk ${chunkNumber} (${Math.round(remainingTime/1000)}s restantes)`);
          console.log(`📊 Progreso actual: ${processedChunks}/${totalChunks} chunks (${totalProcessed}/${deals.length} registros)`);
          
          // Guardar progreso antes de salir
          await saveFileProgress(fileName, {
            totalRecords: deals.length,
            totalChunks: totalChunks,
            processedRecords: totalProcessed,
            lastCompletedChunk: processedChunks,
            status: 'processing'
          });
          
          return res.status(200).send(`⏰ Procesamiento parcial: ${processedChunks}/${totalChunks} chunks completados`);
        }
        
        const result = await sendToHubspot(chunk, `${fileName}_chunk_${chunkNumber}`, remainingTime);
        
        // Marcar chunk como completado
        await markChunkAsCompleted(fileName, chunkNumber, chunk.length);
        processedChunks++;
        totalProcessed += chunk.length;
        
        console.log(`✅ Chunk ${chunkNumber} completado. Progreso: ${processedChunks}/${totalChunks}`);
        
        // Actualizar progreso
        await saveFileProgress(fileName, {
          totalRecords: deals.length,
          totalChunks: totalChunks,
          processedRecords: totalProcessed,
          lastCompletedChunk: processedChunks,
          status: processedChunks === totalChunks ? 'completed' : 'processing'
        });
        
        // Verificar tiempo después de cada chunk
        const newElapsedTime = Date.now() - executionStart;
        if (newElapsedTime > MAX_EXECUTION_TIME * 0.85) {
          console.log(`⏰ Límite de tiempo alcanzado después del chunk ${chunkNumber}`);
          console.log(`📊 Procesados ${processedChunks}/${totalChunks} chunks (${totalProcessed}/${deals.length} registros)`);
          return res.status(200).send(`⏰ Procesamiento parcial: ${processedChunks}/${totalChunks} chunks completados`);
        }
      }
      
      // Solo marcar como completamente procesado si se procesaron todos los chunks
      if (processedChunks === totalChunks) {
        processed.push(fileName);
        await saveFileProgress(fileName, {
          totalRecords: deals.length,
          totalChunks: totalChunks,
          processedRecords: totalProcessed,
          lastCompletedChunk: processedChunks,
          status: 'completed'
        });
        console.log(`✅ Procesado exitosamente: ${fileName} (todos los chunks completados)`);
      } else {
        console.log(`⚠️ Procesamiento parcial: ${fileName} (${processedChunks}/${totalChunks} chunks)`);
        return res.status(200).send(`⚠️ Procesamiento parcial: ${processedChunks}/${totalChunks} chunks completados`);
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
