//Negocios
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
  
  console.log(`\n🚀 ================== INICIO DE SINCRONIZACIÓN ==================`);
  console.log(`⏰ Tiempo máximo de ejecución: ${Math.round(MAX_EXECUTION_TIME/1000)}s`);
  console.log(`🕒 Inicio: ${new Date().toLocaleString('es-EC')}`);
  
  console.log("🔌 Verificando conexión con buckets S3...");

  const s3Ok = await testS3Connections();
  if (!s3Ok) {
    console.error("❌ Error crítico: No se pudo conectar a los buckets S3");
    return res.status(500).send("❌ Fallo en conexión a uno o ambos buckets S3.");
  }
  console.log("✅ Conexión a buckets S3 exitosa");

  console.log("📃 Cargando historial de archivos procesados...");
  const processed = await readProcessedList();
  console.log(`📋 Archivos ya procesados: ${processed.length}`);

  const command = new ListObjectsV2Command({
    Bucket: AWS1_BUCKET,
    Prefix: "delta_negocio_",
  });

  const { Contents = [] } = await s3Read.send(command);
  const nuevosArchivos = Contents.map((obj) => obj.Key)
    .filter((key) => key.endsWith(".csv"))
    .filter((key) => !processed.includes(key))
    .sort(); 

  console.log(`\n📁 ================== ANÁLISIS DE ARCHIVOS ==================`);
  console.log(`📂 Total archivos en bucket: ${Contents.length}`);
  console.log(`📄 Archivos CSV encontrados: ${Contents.filter(obj => obj.Key.endsWith('.csv')).length}`);
  console.log(`✅ Archivos ya procesados: ${processed.length}`);
  console.log(`🆕 Archivos nuevos para procesar: ${nuevosArchivos.length}`);
  
  if (nuevosArchivos.length > 0) {
    console.log(`📋 Lista de archivos nuevos:`);
    nuevosArchivos.slice(0, 5).forEach((file, index) => {
      console.log(`   ${index + 1}. ${file}`);
    });
    if (nuevosArchivos.length > 5) {
      console.log(`   ... y ${nuevosArchivos.length - 5} archivos más`);
    }
  }
  console.log(`===============================================================\n`);

  if (nuevosArchivos.length === 0) {
    console.log("🟡 No hay nuevos archivos para procesar.");
    console.log(`🕒 Ejecución completada en ${((Date.now() - executionStart) / 1000).toFixed(2)}s`);
    return res.status(200).send("🟡 No hay archivos nuevos.");
  }

  const fileName = nuevosArchivos[0];
  
  try {
    const elapsed = Date.now() - executionStart;
    if (elapsed > MAX_EXECUTION_TIME * 0.05) { 
      console.log("⏰ Tiempo insuficiente para procesar archivo");
      return res.status(200).send("⏰ Tiempo insuficiente - reintentar");
    }

    console.log(`\n⬇️ ================== PROCESANDO ARCHIVO ==================`);
    console.log(`📄 Archivo seleccionado: ${fileName}`);
    
    // Verificar si ya tiene progreso parcial
    const fileProgress = await getFileProgress(fileName);
    let deals;
    
    if (fileProgress && fileProgress.totalRecords) {
      console.log(`🔄 REANUDANDO procesamiento de ${fileName}`);
      console.log(`📊 Progreso anterior encontrado:`);
      console.log(`   • Total registros: ${fileProgress.totalRecords}`);
      console.log(`   • Chunks completados: ${fileProgress.lastCompletedChunk}/${fileProgress.totalChunks}`);
      console.log(`   • Registros procesados: ${fileProgress.processedRecords}/${fileProgress.totalRecords}`);
      console.log(`   • Estado: ${fileProgress.status}`);
      console.log(`   • Última actualización: ${fileProgress.lastUpdated}`);
    } else {
      console.log(`🆕 INICIANDO nuevo procesamiento de ${fileName}`);
    }
    
    console.log(`\n📥 Descargando y parseando CSV desde S3...`);
    deals = await fetchCSVFromS3(fileName);

    if (!deals.length) {
      console.warn(`⚠️ ARCHIVO VACÍO detectado: ${fileName}`);
      console.warn(`   • No se encontraron deals válidos para procesar`);
      console.warn(`   • Marcando archivo como procesado`);
      processed.push(fileName);
      await saveProcessedList(processed);
      const warningMsg = `⚠️ Archivo vacío procesado: ${fileName}`;
      console.log(warningMsg);
      return res.status(200).send(warningMsg);
    }

    console.log(`\n📊 ================== ESTRATEGIA DE PROCESAMIENTO ==================`);
    console.log(`📄 Total deals válidos extraídos del CSV: ${deals.length}`);
    
    if (deals.length > 5000) {
      console.log(`📏 ARCHIVO GRANDE detectado (${deals.length} registros) - procesando en chunks`);
      
      const chunkSize = 2500;
      const totalChunks = Math.ceil(deals.length / chunkSize);
      let processedChunks = fileProgress ? fileProgress.lastCompletedChunk : 0;
      let totalProcessed = fileProgress ? fileProgress.processedRecords : 0;
      
      console.log(`📊 Configuración de chunks:`);
      console.log(`   • Tamaño de chunk: ${chunkSize} deals`);
      console.log(`   • Total chunks: ${totalChunks}`);
      console.log(`   • Chunks ya completados: ${processedChunks}`);
      console.log(`   • Chunk inicial: ${processedChunks + 1}`);
      
      // Guardar el total de registros
      if (!fileProgress || !fileProgress.totalRecords) {
        console.log(`💾 Guardando metadata inicial del archivo...`);
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
      console.log(`🔄 Iniciando procesamiento desde chunk ${startChunk + 1}...`);
      
      for (let i = startChunk * chunkSize; i < deals.length; i += chunkSize) {
        const chunk = deals.slice(i, i + chunkSize);
        const chunkNumber = Math.floor(i/chunkSize) + 1;
        
        console.log(`\n🔄 ================== PROCESANDO CHUNK ${chunkNumber}/${totalChunks} ==================`);
        console.log(`📊 Deals en este chunk: ${chunk.length}`);
        console.log(`📈 Progreso general: ${((chunkNumber-1)/totalChunks*100).toFixed(1)}%`);
        
        const elapsedTime = Date.now() - executionStart;
        const remainingTime = MAX_EXECUTION_TIME - elapsedTime;
        
        console.log(`⏰ Tiempo transcurrido: ${Math.round(elapsedTime/1000)}s`);
        console.log(`⏰ Tiempo restante: ${Math.round(remainingTime/1000)}s`);
        
        if (remainingTime < 30000) {
          console.log(`\n⏰ ================== TIMEOUT PREVENTIVO ==================`);
          console.log(`⚠️ Tiempo insuficiente para chunk ${chunkNumber} (${Math.round(remainingTime/1000)}s restantes)`);
          console.log(`📊 Estado actual:`);
          console.log(`   • Chunks completados: ${processedChunks}/${totalChunks}`);
          console.log(`   • Registros procesados: ${totalProcessed}/${deals.length}`);
          console.log(`   • Porcentaje completado: ${((processedChunks/totalChunks)*100).toFixed(1)}%`);
          
          // Guardar progreso antes de salir
          await saveFileProgress(fileName, {
            totalRecords: deals.length,
            totalChunks: totalChunks,
            processedRecords: totalProcessed,
            lastCompletedChunk: processedChunks,
            status: 'processing'
          });
          
          const timeoutMsg = `⏰ Procesamiento parcial: ${processedChunks}/${totalChunks} chunks completados`;
          console.log(`🔄 Necesario ejecutar nuevamente para continuar`);
          return res.status(200).send(timeoutMsg);
        }
        
        console.log(`🚀 Enviando chunk ${chunkNumber} a HubSpot...`);
        const result = await sendToHubspot(chunk, `${fileName}_chunk_${chunkNumber}`, remainingTime);
        
        // chunk como completado
        await markChunkAsCompleted(fileName, chunkNumber, chunk.length);
        processedChunks++;
        totalProcessed += chunk.length;
        
        console.log(`\n✅ ================== CHUNK ${chunkNumber} COMPLETADO ==================`);
        console.log(`✅ Chunk ${chunkNumber}/${totalChunks} procesado exitosamente`);
        console.log(`📊 Progreso actualizado:`);
        console.log(`   • Chunks completados: ${processedChunks}/${totalChunks} (${((processedChunks/totalChunks)*100).toFixed(1)}%)`);
        console.log(`   • Registros procesados: ${totalProcessed}/${deals.length} (${((totalProcessed/deals.length)*100).toFixed(1)}%)`);
        console.log(`   • Deals subidos en este chunk: ${result.totalSubidos || 0}`);
        
        // Actualizar progreso
        await saveFileProgress(fileName, {
          totalRecords: deals.length,
          totalChunks: totalChunks,
          processedRecords: totalProcessed,
          lastCompletedChunk: processedChunks,
          status: processedChunks === totalChunks ? 'completed' : 'processing'
        });
        
        const newElapsedTime = Date.now() - executionStart;
        if (newElapsedTime > MAX_EXECUTION_TIME * 0.85) {
          console.log(`\n⏰ ================== LÍMITE DE TIEMPO ALCANZADO ==================`);
          console.log(`⏰ Límite de tiempo preventivo alcanzado después del chunk ${chunkNumber}`);
          console.log(`⏱️ Tiempo transcurrido: ${Math.round(newElapsedTime/1000)}s de ${Math.round(MAX_EXECUTION_TIME/1000)}s`);
          console.log(`📊 Estado final de esta ejecución:`);
          console.log(`   • Chunks completados: ${processedChunks}/${totalChunks}`);
          console.log(`   • Registros procesados: ${totalProcessed}/${deals.length}`);
          console.log(`   • Progreso: ${((processedChunks/totalChunks)*100).toFixed(1)}%`);
          console.log(`🔄 Se requiere otra ejecución para completar el archivo`);
          
          const partialMsg = `⏰ Procesamiento parcial: ${processedChunks}/${totalChunks} chunks completados`;
          return res.status(200).send(partialMsg);
        }
      }
      
      if (processedChunks === totalChunks) {
        console.log(`\n🎉 ================== ARCHIVO COMPLETAMENTE PROCESADO ==================`);
        console.log(`✅ TODOS los chunks procesados exitosamente`);
        console.log(`📊 Resumen final:`);
        console.log(`   • Total chunks: ${totalChunks}`);
        console.log(`   • Total registros: ${deals.length}`);
        console.log(`   • Archivo: ${fileName}`);
        
        processed.push(fileName);
        await saveFileProgress(fileName, {
          totalRecords: deals.length,
          totalChunks: totalChunks,
          processedRecords: totalProcessed,
          lastCompletedChunk: processedChunks,
          status: 'completed'
        });
        console.log(`✅ Archivo marcado como completamente procesado: ${fileName}`);
      } else {
        console.log(`\n⚠️ ================== PROCESAMIENTO PARCIAL ==================`);
        console.log(`⚠️ Procesamiento parcial completado para esta ejecución`);
        console.log(`📊 Estado: ${fileName} (${processedChunks}/${totalChunks} chunks)`);
        const partialMsg = `⚠️ Procesamiento parcial: ${processedChunks}/${totalChunks} chunks completados`;
        return res.status(200).send(partialMsg);
      }
      
    } else {
      console.log(`📄 Archivo de tamaño normal (${deals.length} registros) - procesamiento directo`);
      const remainingTime = MAX_EXECUTION_TIME - (Date.now() - executionStart);
      console.log(`⏰ Tiempo disponible para procesamiento: ${Math.round(remainingTime/1000)}s`);
      
      await sendToHubspot(deals, fileName, remainingTime);
      processed.push(fileName);
      console.log(`✅ Archivo procesado exitosamente: ${fileName}`);
    }
    
  } catch (error) {
    console.error(`\n❌ ================== ERROR CRÍTICO ==================`);
    console.error(`❌ Error procesando archivo: ${fileName}`);
    console.error(`🔍 Detalles del error:`, error);
    console.error(`📍 Stack trace:`, error.stack);
    console.error(`⏰ Tiempo transcurrido hasta error: ${Math.round((Date.now() - executionStart)/1000)}s`);
    console.error(`========================================================`);
    
    const errorMsg = `❌ Error procesando ${fileName}: ${error.message}`;
    return res.status(500).send(errorMsg);
  }

  console.log(`\n💾 ================== ACTUALIZANDO HISTORIAL ==================`);
  console.log("💾 Guardando lista de archivos procesados...");
  await saveProcessedList(processed);
  console.log(`✅ Historial actualizado. Total archivos procesados: ${processed.length}`);

  const executionTime = ((Date.now() - executionStart) / 1000).toFixed(2);
  const response = `✅ Procesado archivo: ${fileName} en ${executionTime}s. ${nuevosArchivos.length - 1} archivos pendientes.`;
  
  console.log(`\n🎯 ================== EJECUCIÓN COMPLETADA ==================`);
  console.log(`✅ Archivo procesado: ${fileName}`);
  console.log(`⏱️ Tiempo total de ejecución: ${executionTime}s`);
  console.log(`📋 Archivos pendientes: ${nuevosArchivos.length - 1}`);
  console.log(`🕒 Finalización: ${new Date().toLocaleString('es-EC')}`);
  console.log(`===============================================================\n`);
  
  return res.status(200).send(response);
}
