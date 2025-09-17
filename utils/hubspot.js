// utils/hubspot.js
import fetch from "node-fetch";
import { saveReportToS3, savePartialProgress } from "./s3Helpers.js";

const HUBSPOT_BASE = "https://api.hubapi.com";
const BATCH_SIZE = 100;
const MAX_CONCURRENT_REQUESTS = 3; 
const CONTACT_BATCH_SIZE = 100;

export async function sendToHubspot(deals, fileName, maxExecutionTime = 240000) {
  const startTime = Date.now();
  
  const apiKey = process.env.HUBSPOT_API_KEY;
  console.log(`🔑 Usando API Key: ${apiKey ? `${apiKey.substring(0, 10)}...${apiKey.substring(apiKey.length - 4)}` : 'NO CONFIGURADA'}`);
  
  if (!apiKey) {
    throw new Error("❌ HUBSPOT_API_KEY no está configurada");
  }

  const contactIdToDeals = {};

  // Agrupar deals por contact_id
  for (const deal of deals) {
    const contactId = deal.associations?.[0]?.to?.id;
    if (contactId) {
      if (!contactIdToDeals[contactId]) contactIdToDeals[contactId] = [];
      contactIdToDeals[contactId].push(deal);
    }
  }

  const allContactIds = Object.keys(contactIdToDeals);
  console.log(`🔍 Total contactos únicos referenciados: ${allContactIds.length}`);
  console.log(`📊 Total deals a procesar: ${deals.length}`);
  
  // Mostrar sample de contact IDs para debugging
  if (allContactIds.length > 0) {
    console.log(`🔍 Muestra de Contact IDs a validar:`);
    allContactIds.slice(0, 5).forEach((id, index) => {
      console.log(`   ${index + 1}. ${id} (${contactIdToDeals[id].length} deals)`);
    });
    if (allContactIds.length > 5) {
      console.log(`   ... y ${allContactIds.length - 5} contactos más`);
    }
  }

  const timeForContactValidation = Math.min(maxExecutionTime * 0.6, 120000);
  
  const contactIdToHubspotId = await validateContactsInParallel(
    allContactIds, 
    apiKey, 
    timeForContactValidation
  );

  console.log(`\n🔍 ================== VALIDACIÓN DE CONTACTOS ==================`);
  console.log(`✅ Contactos válidos encontrados: ${contactIdToHubspotId.size} de ${allContactIds.length}`);
  console.log(`❌ Contactos no encontrados: ${allContactIds.length - contactIdToHubspotId.size}`);
  
  // Mostrar mapeo de contactos encontrados
  if (contactIdToHubspotId.size > 0) {
    console.log(`🔗 Muestra de mapeo Contact ID -> HubSpot ID:`);
    let count = 0;
    for (const [customId, hubspotId] of contactIdToHubspotId) {
      if (count < 3) {
        console.log(`   • ${customId} -> ${hubspotId}`);
        count++;
      } else {
        break;
      }
    }
    if (contactIdToHubspotId.size > 3) {
      console.log(`   ... y ${contactIdToHubspotId.size - 3} mapeos más`);
    }
  }
  console.log(`===============================================================`);

  const { validDeals, invalidDeals } = separateValidDeals(contactIdToDeals, contactIdToHubspotId);

  if (invalidDeals.length > 0) {
    logInvalidDeals(invalidDeals);
  }

  const remainingTime = maxExecutionTime - (Date.now() - startTime);
  
  const result = await processValidDealsWithTimeout(
    validDeals, 
    apiKey, 
    fileName,
    remainingTime
  );

  await generateFinalReport(deals, result, invalidDeals, fileName);
  
  return result;
}

async function validateContactsInParallel(contactIds, apiKey, maxTime) {
  const startTime = Date.now();
  const contactIdToHubspotId = new Map();
  const chunks = [];
  
  for (let i = 0; i < contactIds.length; i += CONTACT_BATCH_SIZE) {
    chunks.push(contactIds.slice(i, i + CONTACT_BATCH_SIZE));
  }

  console.log(`🔍 Validando ${contactIds.length} contactos en ${chunks.length} chunks con ${Math.round(maxTime/1000)}s disponibles`);

  let totalValidated = 0;
  let totalErrors = 0;

  for (let i = 0; i < chunks.length; i += MAX_CONCURRENT_REQUESTS) {
    const elapsed = Date.now() - startTime;
    if (elapsed > maxTime) {
      console.log(`⏰ Timeout alcanzado durante validación de contactos (${Math.round(elapsed/1000)}s transcurridos)`);
      break;
    }

    const currentBatchChunks = chunks.slice(i, i + MAX_CONCURRENT_REQUESTS);
    console.log(`🔄 Procesando batches ${i + 1}-${Math.min(i + MAX_CONCURRENT_REQUESTS, chunks.length)} de ${chunks.length}`);

    const batchPromises = currentBatchChunks.map(async (chunk, index) => {
      const batchNumber = i + index + 1;
      try {
        console.log(`📡 Enviando batch ${batchNumber} (${chunk.length} contactos) a HubSpot...`);
        
        const requestBody = {
          idProperty: "contact_id",
          inputs: chunk.map((id) => ({ id })),
        };

        console.log(`🔍 Batch ${batchNumber} - Buscando contactos con contact_id: ${chunk.slice(0, 3).join(', ')}${chunk.length > 3 ? '...' : ''}`);

        const res = await fetch(`${HUBSPOT_BASE}/crm/v3/objects/contacts/batch/read`, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${apiKey}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify(requestBody),
        });

        if (!res.ok) {
          const error = await res.text();
          console.error(`❌ Batch ${batchNumber} - Error HTTP ${res.status}:`, error);
          
          // Intentar parsear el error para más detalles
          try {
            const errorObj = JSON.parse(error);
            if (errorObj.message) {
              console.error(`   Mensaje: ${errorObj.message}`);
            }
            if (errorObj.errors) {
              console.error(`   Errores adicionales:`, errorObj.errors);
            }
          } catch (e) {
            // Error no es JSON válido
          }
          
          return { batchNumber, results: [], error: error };
        }

        const data = await res.json();
        const results = data.results || [];
        
        console.log(`✅ Batch ${batchNumber} - Respuesta recibida: ${results.length} contactos encontrados de ${chunk.length} buscados`);
        
        if (results.length > 0) {
          console.log(`🔍 Batch ${batchNumber} - Contactos encontrados:`);
          results.slice(0, 3).forEach(contact => {
            const customId = contact.properties?.contact_id;
            const hubspotId = contact.id;
            const email = contact.properties?.email || 'Sin email';
            console.log(`   • Contact ID: ${customId} -> HubSpot ID: ${hubspotId} (${email})`);
          });
          if (results.length > 3) {
            console.log(`   ... y ${results.length - 3} contactos más`);
          }
        } else {
          console.warn(`⚠️ Batch ${batchNumber} - No se encontraron contactos con los IDs proporcionados`);
        }

        return { batchNumber, results, error: null };

      } catch (err) {
        console.error(`❌ Batch ${batchNumber} - Excepción:`, err.message);
        return { batchNumber, results: [], error: err.message };
      }
    });

    const batchResults = await Promise.all(batchPromises);
    
    // Procesar resultados
    batchResults.forEach(({ batchNumber, results, error }) => {
      if (error) {
        totalErrors++;
        console.error(`❌ Batch ${batchNumber} falló: ${error}`);
      } else {
        totalValidated += results.length;
        results.forEach(contact => {
          const customContactId = contact.properties?.contact_id;
          const hubspotId = contact.id;
          if (customContactId && hubspotId) {
            contactIdToHubspotId.set(customContactId, hubspotId);
          }
        });
      }
    });

    // Mostrar progreso cada 10 batches
    const processedBatches = Math.min(i + MAX_CONCURRENT_REQUESTS, chunks.length);
    if (processedBatches % 10 === 0 || processedBatches === chunks.length) {
      console.log(`📊 Progreso validación: ${processedBatches}/${chunks.length} batches (${totalValidated} contactos válidos, ${totalErrors} errores)`);
    }

    await wait(100); // Rate limiting
  }

  const elapsedTotal = Date.now() - startTime;
  console.log(`✅ Validación completada en ${Math.round(elapsedTotal/1000)}s`);
  console.log(`📊 Resumen validación: ${totalValidated} contactos válidos, ${totalErrors} batches con error`);

  return contactIdToHubspotId;
}

function separateValidDeals(contactIdToDeals, contactIdToHubspotId) {
  const validDeals = [];
  const invalidDeals = [];

  console.log(`\n🔄 ================== SEPARANDO DEALS VÁLIDOS ==================`);

  for (const contactId of Object.keys(contactIdToDeals)) {
    const negocios = contactIdToDeals[contactId];
    const hubspotId = contactIdToHubspotId.get(contactId);

    if (hubspotId) {
      for (const negocio of negocios) {
        // Actualizar el ID de asociación con el ID real de HubSpot
        negocio.associations[0].to.id = hubspotId;
        validDeals.push(negocio);
      }
    } else {
      for (const negocio of negocios) {
        invalidDeals.push({
          dealName: negocio.properties.dealname || "Sin nombre",
          contactId,
          originalContactId: negocio.properties.contact_id_original
        });
      }
    }
  }

  console.log(`✅ Deals válidos (con contacto existente): ${validDeals.length}`);
  console.log(`❌ Deals inválidos (sin contacto): ${invalidDeals.length}`);
  console.log(`===============================================================`);

  return { validDeals, invalidDeals };
}

function logInvalidDeals(invalidDeals) {
  console.log(`\n⚠️ ================== DEALS NO PROCESABLES ==================`);
  console.log(`❌ NEGOCIOS QUE NO SE SUBIRÁN (${invalidDeals.length} total):`);
  console.log(`   Razón: Contactos no encontrados en HubSpot`);
  
  const samplesToShow = Math.min(invalidDeals.length, 5); 
  for (let i = 0; i < samplesToShow; i++) {
    const { dealName, contactId, originalContactId } = invalidDeals[i];
    console.warn(`   ${i + 1}. "${dealName}"`);
    console.warn(`      Contact ID buscado: ${contactId}`);
    if (originalContactId && originalContactId !== contactId) {
      console.warn(`      Contact ID original: ${originalContactId}`);
    }
  }
  if (invalidDeals.length > 5) {
    console.warn(`   ... y ${invalidDeals.length - 5} deals más sin contacto válido`);
  }
  
  // Análisis de IDs faltantes
  const missingContactIds = [...new Set(invalidDeals.map(d => d.contactId))];
  console.log(`📊 Total contactos únicos no encontrados: ${missingContactIds.length}`);
  console.log(`💡 Sugerencia: Verificar que estos contactos existan en HubSpot con el campo 'contact_id' configurado`);
  console.log(`===============================================================`);
}

async function processValidDealsWithTimeout(validDeals, apiKey, fileName, remainingTime) {
  let totalSubidos = 0;
  let totalFallidos = 0;
  const startTime = Date.now();
  const createdDeals = []; // Para logging de deals creados exitosamente

  if (validDeals.length === 0) {
    console.log("⚠️ No hay negocios válidos para subir.");
    return { totalSubidos, totalFallidos };
  }

  console.log(`\n🚀 ================== CREANDO DEALS EN HUBSPOT ==================`);
  console.log(`📊 Enviando ${validDeals.length} negocios válidos a HubSpot`);
  console.log(`⏰ Tiempo disponible: ${Math.round(remainingTime/1000)}s`);
  console.log(`📦 Tamaño de batch: ${BATCH_SIZE} deals por request`);
  
  const totalBatches = Math.ceil(validDeals.length / BATCH_SIZE);
  console.log(`🔢 Total batches a procesar: ${totalBatches}`);

  for (let i = 0; i < validDeals.length; i += BATCH_SIZE) {
    const elapsed = Date.now() - startTime;
    const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
    
    if (elapsed > remainingTime * 0.9) { 
      console.log(`\n⏰ ================== TIMEOUT PREVENTIVO ==================`);
      console.log(`⏰ Límite de tiempo alcanzado en batch ${batchNumber}`);
      console.log(`⏱️ Tiempo transcurrido: ${Math.round(elapsed/1000)}s de ${Math.round(remainingTime/1000)}s`);
      console.log(`📊 Estado al momento del timeout:`);
      console.log(`   • Deals procesados: ${totalSubidos} de ${validDeals.length}`);
      console.log(`   • Batches completados: ${batchNumber - 1} de ${totalBatches}`);
      console.log(`   • Deals fallidos: ${totalFallidos}`);
      
      await savePartialProgress(fileName, totalSubidos, validDeals.length);
      break;
    }

    const batch = validDeals.slice(i, i + BATCH_SIZE);
    const dealRange = `${i + 1}-${i + batch.length}`;

    console.log(`\n📦 Procesando batch ${batchNumber}/${totalBatches} (deals ${dealRange})`);
    
    // Log de sample de deals en el batch para debugging
    console.log(`🔍 Muestra de deals en batch ${batchNumber}:`);
    batch.slice(0, 2).forEach((deal, index) => {
      console.log(`   ${index + 1}. "${deal.properties.dealname}" -> Contacto: ${deal.associations[0].to.id}`);
    });
    if (batch.length > 2) {
      console.log(`   ... y ${batch.length - 2} deals más`);
    }

    try {
      const requestBody = { inputs: batch };
      
      console.log(`📡 Enviando batch ${batchNumber} a HubSpot API...`);
      const res = await fetch(`${HUBSPOT_BASE}/crm/v3/objects/deals/batch/create`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(requestBody),
      });

      if (!res.ok) {
        const error = await res.text();
        console.error(`❌ Batch ${batchNumber} - Error HTTP ${res.status}:`);
        console.error(`   Response: ${error}`);
        
        // Intentar parsear errores específicos de HubSpot
        try {
          const errorObj = JSON.parse(error);
          if (errorObj.message) {
            console.error(`   Mensaje: ${errorObj.message}`);
          }
          if (errorObj.errors) {
            console.error(`   Errores detallados:`);
            errorObj.errors.slice(0, 3).forEach((err, idx) => {
              console.error(`     ${idx + 1}. ${err.message || err}`);
            });
          }
        } catch (e) {
          // Error response no es JSON válido
        }
        
        totalFallidos += batch.length;
        continue;
      }

      // Procesar respuesta exitosa
      const responseData = await res.json();
      const createdInThisBatch = responseData.results || [];
      
      console.log(`✅ Batch ${batchNumber} - Éxito: ${createdInThisBatch.length} deals creados`);
      
      // Log de deals creados exitosamente
      if (createdInThisBatch.length > 0) {
        console.log(`🎯 Deals creados en batch ${batchNumber}:`);
        createdInThisBatch.slice(0, 3).forEach((deal, index) => {
          const dealName = deal.properties?.dealname || 'Sin nombre';
          const dealId = deal.id;
          console.log(`   ✅ ${index + 1}. "${dealName}" (ID: ${dealId})`);
        });
        if (createdInThisBatch.length > 3) {
          console.log(`   ... y ${createdInThisBatch.length - 3} deals más`);
        }
        
        createdDeals.push(...createdInThisBatch);
      }
      
      totalSubidos += createdInThisBatch.length;
      
      // Si algunos deals fallaron en el batch
      if (createdInThisBatch.length < batch.length) {
        const failedInBatch = batch.length - createdInThisBatch.length;
        totalFallidos += failedInBatch;
        console.warn(`⚠️ Batch ${batchNumber} - ${failedInBatch} deals fallaron en la creación`);
      }

      // Guardar progreso cada 5 batches
      if (batchNumber % 5 === 0) {
        console.log(`💾 Guardando progreso intermedio...`);
        await savePartialProgress(fileName, totalSubidos, validDeals.length);
      }

    } catch (err) {
      console.error(`❌ Batch ${batchNumber} - Excepción durante creación:`);
      console.error(`   Error: ${err.message}`);
      console.error(`   Stack: ${err.stack}`);
      totalFallidos += batch.length;
    }

    // Mostrar progreso cada 10 batches
    if (batchNumber % 10 === 0 || batchNumber === totalBatches) {
      const progressPercent = ((batchNumber / totalBatches) * 100).toFixed(1);
      console.log(`📊 Progreso: ${batchNumber}/${totalBatches} batches (${progressPercent}%) - Creados: ${totalSubidos}, Fallidos: ${totalFallidos}`);
    }

    await wait(200); // Rate limiting
  }

  console.log(`\n🎯 ================== RESUMEN CREACIÓN DE DEALS ==================`);
  console.log(`✅ Total deals creados exitosamente: ${totalSubidos}`);
  console.log(`❌ Total deals fallidos: ${totalFallidos}`);
  console.log(`📊 Tasa de éxito en creación: ${validDeals.length > 0 ? ((totalSubidos / validDeals.length) * 100).toFixed(1) : 0}%`);
  console.log(`⏱️ Tiempo utilizado: ${Math.round((Date.now() - startTime) / 1000)}s`);
  console.log(`===============================================================`);

  return { totalSubidos, totalFallidos, createdDeals };
}

async function generateFinalReport(deals, result, invalidDeals, fileName) {
  const { totalSubidos, totalFallidos } = result;
  const totalOriginal = deals.length;
  const totalSinContacto = invalidDeals.length;

  console.log(`\n🎯 ================== RESUMEN FINAL COMPLETO ==================`);
  console.log(`📄 Archivo procesado: ${fileName}`);
  console.log(`📊 Total negocios en archivo: ${totalOriginal}`);
  console.log(`✅ Subidos exitosamente: ${totalSubidos}`);
  console.log(`❌ Fallidos en envío: ${totalFallidos}`);
  console.log(`🚫 Sin contacto válido: ${totalSinContacto}`);
  console.log(`📈 Tasa de éxito total: ${((totalSubidos / totalOriginal) * 100).toFixed(1)}%`);
  
  if (totalOriginal > 0) {
    console.log(`\n📊 Desglose porcentual:`);
    console.log(`   • Creados: ${((totalSubidos / totalOriginal) * 100).toFixed(1)}%`);
    console.log(`   • Fallidos en API: ${((totalFallidos / totalOriginal) * 100).toFixed(1)}%`);
    console.log(`   • Sin contacto: ${((totalSinContacto / totalOriginal) * 100).toFixed(1)}%`);
  }
  
  console.log(`\n💡 Recomendaciones:`);
  if (totalSinContacto > 0) {
    console.log(`   • Verificar que los ${totalSinContacto} contactos existan en HubSpot`);
    console.log(`   • Confirmar que tengan el campo 'contact_id' configurado correctamente`);
  }
  if (totalFallidos > 0) {
    console.log(`   • Revisar logs de API para identificar errores en ${totalFallidos} deals`);
    console.log(`   • Verificar permisos del token de API para creación de deals`);
  }
  console.log(`==============================================================\n`);

  const now = new Date();
  const reportString = `📄 REPORTE DE PROCESAMIENTO - ${fileName || "Archivo desconocido"}

🕒 Fecha de ejecución: ${now.toLocaleDateString("es-EC")} ${now.toLocaleTimeString("es-EC")}

📊 RESUMEN GENERAL:
• Total negocios en archivo: ${totalOriginal}
• Subidos exitosamente: ${totalSubidos}
• Fallidos en envío: ${totalFallidos}  
• Sin contacto válido: ${totalSinContacto}

📈 MÉTRICAS:
• Tasa de éxito total: ${((totalSubidos / totalOriginal) * 100).toFixed(1)}%
• Tasa de éxito (solo válidos): ${totalOriginal - totalSinContacto > 0 ? ((totalSubidos / (totalOriginal - totalSinContacto)) * 100).toFixed(1) : 0}%

🔍 ANÁLISIS:
${totalSinContacto > 0 ? `• ${totalSinContacto} negocios no pudieron procesarse por contactos inexistentes` : '• Todos los contactos fueron encontrados'}
${totalFallidos > 0 ? `• ${totalFallidos} negocios fallaron durante la creación en HubSpot` : '• No hubo fallos en la API de HubSpot'}
${totalSubidos > 0 ? `• ${totalSubidos} negocios creados exitosamente en HubSpot` : '• No se crearon negocios'}

💡 RECOMENDACIONES:
${totalSinContacto > 0 ? '• Revisar y sincronizar base de contactos\n• Verificar campo contact_id en HubSpot' : ''}
${totalFallidos > 0 ? '• Revisar logs detallados de errores de API\n• Verificar permisos del token' : ''}
`.trim();

  const baseFileName = (fileName || `archivo_${now.getTime()}`).replace(".csv", "");
  await saveReportToS3(reportString, `reporte_${baseFileName}.txt`);
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
