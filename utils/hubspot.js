// utils/hubspot.js
import fetch from "node-fetch";
import { saveReportToS3, savePartialProgress } from "./s3Helpers.js";

const HUBSPOT_BASE = "https://api.hubapi.com";
const BATCH_SIZE = 100;
const MAX_CONCURRENT_REQUESTS = 5; // Aumentar concurrencia
const CONTACT_BATCH_SIZE = 300; // Aumentar batch size para contactos

export async function sendToHubspot(deals, fileName) {
  const startTime = Date.now();
  const MAX_EXECUTION_TIME = 250000; // 250 segundos (50s de margen)
  
  const apiKey = process.env.HUBSPOT_API_KEY;
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

  // Validar contactos en paralelo con límite de concurrencia
  const contactIdToHubspotId = await validateContactsInParallel(
    allContactIds, 
    apiKey, 
    MAX_EXECUTION_TIME - (Date.now() - startTime)
  );

  console.log(`✅ Contactos válidos encontrados: ${contactIdToHubspotId.size} de ${allContactIds.length}`);

  // Separar deals válidos e inválidos
  const { validDeals, invalidDeals } = separateValidDeals(contactIdToDeals, contactIdToHubspotId);

  if (invalidDeals.length > 0) {
    logInvalidDeals(invalidDeals);
  }

  // Procesar deals válidos en chunks con timeout
  const result = await processValidDealsWithTimeout(
    validDeals, 
    apiKey, 
    fileName,
    MAX_EXECUTION_TIME - (Date.now() - startTime)
  );

  // Generar reporte final
  await generateFinalReport(deals, result, invalidDeals, fileName);
  
  return result;
}

async function validateContactsInParallel(contactIds, apiKey, maxTime) {
  const contactIdToHubspotId = new Map();
  const chunks = [];
  const startTime = Date.now();
  
  // Dividir en chunks más grandes para reducir requests
  const OPTIMIZED_BATCH_SIZE = 300; // Aumentar batch size
  for (let i = 0; i < contactIds.length; i += OPTIMIZED_BATCH_SIZE) {
    chunks.push(contactIds.slice(i, i + OPTIMIZED_BATCH_SIZE));
  }

  console.log(`🔍 Validando ${contactIds.length} contactos en ${chunks.length} chunks de ${OPTIMIZED_BATCH_SIZE}`);

  // Procesar chunks con mayor concurrencia pero con límite de tiempo estricto
  const MAX_CONCURRENT = 5; // Aumentar concurrencia
  
  for (let i = 0; i < chunks.length; i += MAX_CONCURRENT) {
    // Verificar tiempo restante ANTES de cada batch
    const elapsed = Date.now() - startTime;
    const timePerChunk = elapsed / Math.max(i / MAX_CONCURRENT, 1); // Tiempo promedio por grupo
    const estimatedTimeRemaining = timePerChunk * Math.ceil((chunks.length - i) / MAX_CONCURRENT);
    
    if (elapsed > maxTime * 0.4 || estimatedTimeRemaining > (maxTime - elapsed)) { // Usar máximo 40% del tiempo total
      console.log(`⏰ Timeout preventivo en validación: procesados ${i}/${chunks.length} chunks en ${elapsed}ms`);
      break;
    }

    const batchPromises = chunks.slice(i, i + MAX_CONCURRENT)
      .map(async (chunk, index) => {
        try {
          const res = await Promise.race([
            fetch(`${HUBSPOT_BASE}/crm/v3/objects/contacts/batch/read`, {
              method: "POST",
              headers: {
                Authorization: `Bearer ${apiKey}`,
                "Content-Type": "application/json",
              },
              body: JSON.stringify({
                idProperty: "contact_id",
                inputs: chunk.map((id) => ({ id })),
              }),
            }),
            new Promise((_, reject) => 
              setTimeout(() => reject(new Error('Request timeout')), 8000) // 8s timeout por request
            )
          ]);

          if (!res.ok) {
            const error = await res.text();
            console.error(`❌ Error chunk ${i + index}: ${res.status}`);
            return [];
          }

          const data = await res.json();
          return data.results || [];
        } catch (err) {
          console.error(`❌ Timeout/Error chunk ${i + index}:`, err.message);
          return [];
        }
      });

    const results = await Promise.allSettled(batchPromises);
    
    // Procesar solo resultados exitosos
    results.forEach(result => {
      if (result.status === 'fulfilled') {
        result.value.forEach(contact => {
          const customContactId = contact.properties?.contact_id;
          const hubspotId = contact.id;
          if (customContactId && hubspotId) {
            contactIdToHubspotId.set(customContactId, hubspotId);
          }
        });  
      }
    });

    // Pausa más corta
    await wait(50);
  }

  const validationTime = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`✅ Validación completada en ${validationTime}s: ${contactIdToHubspotId.size}/${contactIds.length} contactos válidos`);
  
  return contactIdToHubspotId;
}

function separateValidDeals(contactIdToDeals, contactIdToHubspotId) {
  const validDeals = [];
  const invalidDeals = [];

  for (const contactId of Object.keys(contactIdToDeals)) {
    const negocios = contactIdToDeals[contactId];
    const hubspotId = contactIdToHubspotId.get(contactId);

    if (hubspotId) {
      for (const negocio of negocios) {
        negocio.associations[0].to.id = hubspotId;
        validDeals.push(negocio);
      }
    } else {
      for (const negocio of negocios) {
        invalidDeals.push({
          dealName: negocio.properties.dealname || "Sin nombre",
          contactId,
        });
      }
    }
  }

  return { validDeals, invalidDeals };
}

function logInvalidDeals(invalidDeals) {
  console.log(`⚠️ NEGOCIOS QUE NO SE SUBIRÁN (${invalidDeals.length}):`);
  const samplesToShow = Math.min(invalidDeals.length, 5); // Reducir logs
  for (let i = 0; i < samplesToShow; i++) {
    const { dealName, contactId } = invalidDeals[i];
    console.warn(`   • "${dealName}" - Contacto inexistente: ${contactId}`);
  }
  if (invalidDeals.length > 5) {
    console.warn(`   ... y ${invalidDeals.length - 5} más`);
  }
}

async function processValidDealsWithTimeout(validDeals, apiKey, fileName, remainingTime) {
  let totalSubidos = 0;
  let totalFallidos = 0;
  const startTime = Date.now();

  if (validDeals.length === 0) {
    console.log("⚠️ No hay negocios válidos para subir.");
    return { totalSubidos, totalFallidos };
  }

  console.log(`🚀 Enviando ${validDeals.length} negocios válidos a HubSpot...`);

  // Procesar con batches más grandes y menos delays
  const OPTIMIZED_BATCH_SIZE = 100;
  
  for (let i = 0; i < validDeals.length; i += OPTIMIZED_BATCH_SIZE) {
    // Verificar tiempo restante
    const elapsed = Date.now() - startTime;
    if (elapsed > remainingTime * 0.85) { // Usar 85% del tiempo disponible
      console.log(`⏰ Timeout preventivo: procesados ${totalSubidos} de ${validDeals.length} negocios`);
      await savePartialProgress(fileName, totalSubidos, validDeals.length);
      break;
    }

    const batch = validDeals.slice(i, i + OPTIMIZED_BATCH_SIZE);

    try {
      const res = await Promise.race([
        fetch(`${HUBSPOT_BASE}/crm/v3/objects/deals/batch/create`, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${apiKey}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ inputs: batch }),
        }),
        new Promise((_, reject) => 
          setTimeout(() => reject(new Error('Request timeout')), 15000) // 15s timeout por batch
        )
      ]);

      if (!res.ok) {
        const error = await res.text();
        console.error(`❌ Error al subir batch ${i}-${i + batch.length - 1}: ${res.status}`);
        totalFallidos += batch.length;
        continue;
      }

      console.log(`✅ Subido batch ${i}-${i + batch.length - 1}`);
      totalSubidos += batch.length;

      // Guardar progreso cada 10 batches
      if ((i / OPTIMIZED_BATCH_SIZE) % 10 === 0) {
        await savePartialProgress(fileName, totalSubidos, validDeals.length);
      }

    } catch (err) {
      console.error(`❌ Timeout/Error batch ${i}-${i + batch.length - 1}:`, err.message);
      totalFallidos += batch.length;
    }

    // Pausa mínima para evitar rate limits
    await wait(100);
  }

  return { totalSubidos, totalFallidos };
}

async function generateFinalReport(deals, result, invalidDeals, fileName) {
  const { totalSubidos, totalFallidos } = result;
  const totalOriginal = deals.length;
  const totalSinContacto = invalidDeals.length;

  console.log(`\n🎯 ================== RESUMEN FINAL ==================`);
  console.log(`📄 Total negocios en archivo: ${totalOriginal}`);
  console.log(`✅ Subidos exitosamente: ${totalSubidos}`);
  console.log(`❌ Fallidos en envío: ${totalFallidos}`);
  console.log(`🚫 Sin contacto válido: ${totalSinContacto}`);
  console.log(`📊 Tasa de éxito: ${((totalSubidos / totalOriginal) * 100).toFixed(1)}%`);
  console.log(`==================================================\n`);

  const now = new Date();
  const reportString = `📄 Procesado archivo: ${fileName || "Desconocido"}

📊 Total negocios en archivo: ${totalOriginal}
✅ Subidos exitosamente: ${totalSubidos}
❌ Fallidos en envío: ${totalFallidos}
🚫 Sin contacto válido: ${totalSinContacto}

📈 Tasa de éxito: ${((totalSubidos / totalOriginal) * 100).toFixed(1)}%

🕒 Fecha de ejecución: ${now.toLocaleDateString("es-EC")} ${now.toLocaleTimeString("es-EC")}
`.trim();

  const baseFileName = (fileName || `archivo_${now.getTime()}`).replace(".csv", "");
  await saveReportToS3(reportString, `reporte_${baseFileName}.txt`);
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
