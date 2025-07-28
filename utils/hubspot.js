//Negocios
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

  const timeForContactValidation = Math.min(maxExecutionTime * 0.6, 120000);
  
  const contactIdToHubspotId = await validateContactsInParallel(
    allContactIds, 
    apiKey, 
    timeForContactValidation
  );

  console.log(`✅ Contactos válidos encontrados: ${contactIdToHubspotId.size} de ${allContactIds.length}`);

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

  for (let i = 0; i < chunks.length; i += MAX_CONCURRENT_REQUESTS) {
    const elapsed = Date.now() - startTime;
    if (elapsed > maxTime) {
      console.log(`⏰ Timeout alcanzado durante validación de contactos (${Math.round(elapsed/1000)}s transcurridos)`);
      break;
    }

    const batchPromises = chunks.slice(i, i + MAX_CONCURRENT_REQUESTS)
      .map(async (chunk, index) => {
        try {
          const res = await fetch(`${HUBSPOT_BASE}/crm/v3/objects/contacts/batch/read`, {
            method: "POST",
            headers: {
              Authorization: `Bearer ${apiKey}`,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              idProperty: "contact_id",
              inputs: chunk.map((id) => ({ id })),
            }),
          });

          if (!res.ok) {
            const error = await res.text();
            console.error(`❌ Error al consultar contactos batch ${i + index + 1}:`, error);
            return [];
          }

          const data = await res.json();
          return data.results || [];
        } catch (err) {
          console.error(`❌ Excepción al consultar batch de contactos:`, err);
          return [];
        }
      });

    const results = await Promise.all(batchPromises);
    
    results.flat().forEach(contact => {
      const customContactId = contact.properties.contact_id;
      const hubspotId = contact.id;
      contactIdToHubspotId.set(customContactId, hubspotId);
    });

    // Mostrar progreso
    if ((i / MAX_CONCURRENT_REQUESTS) % 10 === 0) {
      const processed = Math.min(i + MAX_CONCURRENT_REQUESTS, chunks.length);
      console.log(`🔍 Validados ${processed}/${chunks.length} batches de contactos`);
    }

    await wait(100);
  }

  const elapsedTotal = Date.now() - startTime;
  console.log(`✅ Validación completada en ${Math.round(elapsedTotal/1000)}s`);

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
  const samplesToShow = Math.min(invalidDeals.length, 5); 
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

  console.log(`🚀 Enviando ${validDeals.length} negocios válidos a HubSpot con ${Math.round(remainingTime/1000)}s disponibles...`);

  for (let i = 0; i < validDeals.length; i += BATCH_SIZE) {
    const elapsed = Date.now() - startTime;
    if (elapsed > remainingTime * 0.9) { 
      console.log(`⏰ Timeout preventivo: procesados ${totalSubidos} de ${validDeals.length} negocios (${Math.round(elapsed/1000)}s transcurridos)`);
      await savePartialProgress(fileName, totalSubidos, validDeals.length);
      break;
    }

    const batch = validDeals.slice(i, i + BATCH_SIZE);

    try {
      const res = await fetch(`${HUBSPOT_BASE}/crm/v3/objects/deals/batch/create`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ inputs: batch }),
      });

      if (!res.ok) {
        const error = await res.text();
        console.error(`❌ Error al subir batch ${i}-${i + batch.length - 1}:`, error);
        totalFallidos += batch.length;
        continue;
      }

      console.log(`✅ Subido batch ${i}-${i + batch.length - 1}`);
      totalSubidos += batch.length;

      // Guardar progreso cada 5 batches
      if ((i / BATCH_SIZE) % 5 === 0) {
        await savePartialProgress(fileName, totalSubidos, validDeals.length);
      }

    } catch (err) {
      console.error(`❌ Excepción al subir batch ${i}-${i + batch.length - 1}:`, err);
      totalFallidos += batch.length;
    }

    await wait(200);
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
