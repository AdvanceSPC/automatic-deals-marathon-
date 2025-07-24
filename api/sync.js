// api/sync.js
import {
  fetchCSVFromS3,
  readProcessedList,
  saveProcessedList,
  testS3Connections,
  getPartialFiles,
  savePartialFileProgress,
  removePartialFile,
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
  const MAX_EXECUTION_TIME = 280000;
  
  console.log("🔌 Verificando conexión con buckets S3...");

  const s3Ok = await testS3Connections();
  if (!s3Ok) {
    return res.status(500).send("❌ Fallo en conexión a uno o ambos buckets S3.");
  }

  console.log("📃 Cargando historial...");
  const processed = await readProcessedList();
  
  console.log("🔄 Verificando archivos parciales pendientes...");
  const partialFiles = await getPartialFiles();

  const command = new ListObjectsV2Command({
    Bucket: AWS1_BUCKET,
    Prefix: "delta_negocio_",
  });

  const { Contents = [] } = await s3Read.send(command);
  const nuevosArchivos = Contents.map((obj) => obj.Key)
    .filter((key) => key.endsWith(".csv"))
    .filter((key) => !processed.includes(key))
    .sort(); 

  const archivosParaProcesar = [];
  
  // Agregar archivos parciales pendientes
  for (const partial of partialFiles) {
    archivosParaProcesar.push({
      fileName: partial.fileName,
      type: 'partial',
      startIndex: partial.nextStartIndex,
      totalRecords: partial.totalRecords
    });
  }
  
  // Agregar archivos nuevos
  for (const fileName of nuevosArchivos) {
    archivosParaProcesar.push({
      fileName,
      type: 'new',
      startIndex: 0
    });
  }

  if (archivosParaProcesar.length === 0) {
    console.log("🟡 No hay archivos para procesar.");
    return res.status(200).send("🟡 No hay archivos nuevos ni parciales pendientes.");
  }

  console.log(`📁 Total archivos para procesar: ${archivosParaProcesar.length} (${partialFiles.length} parciales, ${nuevosArchivos.length} nuevos)`);

  let archivosCompletados = 0;
  let archivosParciales = 0;
  const resultados = [];

  for (const archivoInfo of archivosParaProcesar) {
    const tiempoRestante = MAX_EXECUTION_TIME - (Date.now() - executionStart);
    const { fileName, type, startIndex, totalRecords } = archivoInfo;
    
    if (tiempoRestante < 60000) {
      console.log(`⏰ Tiempo insuficiente (${Math.round(tiempoRestante/1000)}s) para procesar más archivos`);
      break;
    }

    try {
      if (type === 'partial') {
        console.log(`🔄 Continuando archivo parcial: ${fileName} desde registro ${startIndex}/${totalRecords}`);
      } else {
        console.log(`⬇️ Procesando archivo nuevo: ${fileName} (${Math.round(tiempoRestante/1000)}s restantes)`);
      }
      
      const deals = await fetchCSVFromS3(fileName);

      if (!deals.length) {
        console.warn(`⚠️ Archivo vacío: ${fileName}`);
        if (type === 'partial') await removePartialFile(fileName);
        processed.push(fileName);
        archivosCompletados++;
        resultados.push({ archivo: fileName, estado: 'vacío', registros: 0 });
        continue;
      }

      const dealsToProcess = type === 'partial' ? deals.slice(startIndex) : deals;
      console.log(`📨 Enviando ${dealsToProcess.length} negocios a HubSpot...`);
      const tiempoEstimadoPorRegistro = 0.03;
      const tiempoEstimado = dealsToProcess.length * tiempoEstimadoPorRegistro * 1000;
      const tiempoDisponibleParaArchivo = tiempoRestante - 30000; 
      
      if (tiempoEstimado > tiempoDisponibleParaArchivo && dealsToProcess.length > 1000) {
        console.log(`📏 Archivo grande: ${dealsToProcess.length} registros pendientes, tiempo estimado: ${Math.round(tiempoEstimado/1000)}s`);
        
        const registrosPorSegundo = 1000 / (tiempoEstimadoPorRegistro * 1000);
        const registrosAProcesar = Math.floor((tiempoDisponibleParaArchivo / 1000) * registrosPorSegundo * 0.8); // Factor de seguridad
        
        console.log(`🎯 Procesando ${registrosAProcesar} de ${dealsToProcess.length} registros restantes`);
        
        const chunk = dealsToProcess.slice(0, registrosAProcesar);
        const resultado = await sendToHubspot(chunk, `${fileName}_partial`);
        
        // Actualizar progreso parcial
        const nuevoStartIndex = startIndex + registrosAProcesar;
        await savePartialFileProgress(fileName, nuevoStartIndex, deals.length);
        
        archivosParciales++;
        resultados.push({ 
          archivo: fileName, 
          estado: 'parcial', 
          registros: `${nuevoStartIndex}/${deals.length}`,
          subidos: resultado.totalSubidos 
        });
        
      } else {
        const resultado = await sendToHubspot(dealsToProcess, fileName);
        if (type === 'partial') {
          await removePartialFile(fileName);
        }
        processed.push(fileName);
        archivosCompletados++;
        
        const totalProcesado = type === 'partial' ? deals.length : dealsToProcess.length;
        resultados.push({ 
          archivo: fileName, 
          estado: 'completo', 
          registros: totalProcesado,
          subidos: resultado.totalSubidos 
        });
      }

      console.log(`✅ Procesado: ${fileName}`);
      
    } catch (error) {
      console.error(`❌ Error procesando ${fileName}:`, error);
      resultados.push({ 
        archivo: fileName, 
        estado: 'error', 
        error: error.message 
      });
    }

    const tiempoTranscurrido = Date.now() - executionStart;
    if (tiempoTranscurrido > MAX_EXECUTION_TIME * 0.9) {
      console.log(`⏰ Alcanzado 90% del tiempo límite (${Math.round(tiempoTranscurrido/1000)}s)`);
      break;
    }
  }

  console.log("💾 Actualizando historial...");
  await saveProcessedList(processed);

  const partialFilesRestantes = await getPartialFiles();

  const executionTime = ((Date.now() - executionStart) / 1000).toFixed(2);
  const totalSubidos = resultados.reduce((sum, r) => sum + (r.subidos || 0), 0);
  
  const resumen = `
🎯 ================ RESUMEN DE EJECUCIÓN ================
⏱️  Tiempo de ejecución: ${executionTime}s
📁 Archivos encontrados: ${archivosParaProcesar.length}
✅ Archivos completados: ${archivosCompletados}
🔄 Archivos parciales: ${archivosParciales}
📊 Total registros subidos: ${totalSubidos}

📋 Detalle por archivo:
${resultados.map(r => `   • ${r.archivo}: ${r.estado} ${r.registros ? `(${r.registros} registros)` : ''} ${r.subidos ? `→ ${r.subidos} subidos` : ''}`).join('\n')}

${partialFilesRestantes.length > 0 ? `🔄 Archivos parciales pendientes: ${partialFilesRestantes.length}` : ''}
${nuevosArchivos.length - archivosCompletados > 0 ? `⏳ Archivos nuevos pendientes: ${nuevosArchivos.length - archivosCompletados}` : ''}
${partialFilesRestantes.length === 0 && nuevosArchivos.length - archivosCompletados === 0 ? '🎉 Todos los archivos procesados completamente' : ''}
======================================================`.trim();

  console.log(resumen);
  return res.status(200).send(resumen);
}