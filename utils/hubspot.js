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

  if (contactIdToHubspotId.size > 0) {
    console.log(`🔗 Muestra de mapeo Contact ID -> HubSpot ID:`);
    let count = 0;
    for (const [customId, hubspotId] of contactIdToHubspotId) {
      if (count < 3) {
        console.log(`   • ${customId} -> ${hubspotId}`);
        count++;
      } else break;
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

  console.log(`🔍 Validando ${contactIds.length} contactos en ${chunks.length} chunks con ${Math.round(maxTime / 1000)}s disponibles`);

  let totalValidated = 0;
  let totalErrors = 0;

  for (let i = 0; i < chunks.length; i += MAX_CONCURRENT_REQUESTS) {
    const elapsed = Date.now() - startTime;
    if (elapsed > maxTime) {
      console.log(`⏰ Timeout alcanzado durante validación de contactos (${Math.round(elapsed / 1000)}s transcurridos)`);
      break;
    }

    const currentBatchChunks = chunks.slice(i, i + MAX_CONCURRENT_REQUESTS);
    console.log(`🔄 Procesando batches ${i + 1}-${Math.min(i + MAX_CONCURRENT_REQUESTS, chunks.length)} de ${chunks.length}`);

    const batchPromises = currentBatchChunks.map(async (chunk, index) => {
      const batchNumber = i + index + 1;
      try {
        const requestBody = {
          idProperty: "contact_id",
          inputs: chunk.map((id) => ({ id })),
        };

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
          return { batchNumber, results: [], error: error };
        }

        const data = await res.json();
        const results = data.results || [];

        console.log(`✅ Batch ${batchNumber} - ${results.length} contactos encontrados`);
        return { batchNumber, results, error: null };
      } catch (err) {
        console.error(`❌ Batch ${batchNumber} - Excepción:`, err.message);
        return { batchNumber, results: [], error: err.message };
      }
    });

    const batchResults = await Promise.all(batchPromises);

    batchResults.forEach(({ results, error }) => {
      if (error) totalErrors++;
      else {
        totalValidated += results.length;
        results.forEach(contact => {
          const customId = contact.properties?.contact_id;
          const hubspotId = contact.id;
          if (customId && hubspotId) contactIdToHubspotId.set(customId, hubspotId);
        });
      }
    });

    await wait(100);
  }

  console.log(`✅ Validación completada: ${totalValidated} válidos, ${totalErrors} con error`);
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
        negocio.associations[0].to.id = hubspotId;
        validDeals.push(negocio);
      }
    } else {
      for (const negocio of negocios) {
        invalidDeals.push({
          dealName: negocio.properties.dealname || "Sin nombre",
          contactId,
          originalContactId: negocio.properties.contact_id_original,
        });
      }
    }
  }

  console.log(`✅ Deals válidos: ${validDeals.length}`);
  console.log(`❌ Deals inválidos: ${invalidDeals.length}`);
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
  console.log(`===============================================================`);
}

async function processValidDealsWithTimeout(validDeals, apiKey, fileName, remainingTime) {
  let totalSubidos = 0;
  let totalFallidos = 0;
  const startTime = Date.now();

  if (validDeals.length === 0) {
    console.log("⚠️ No hay negocios válidos para subir.");
    return { totalSubidos, totalFallidos };
  }

  console.log(`\n🚀 ================== CREANDO DEALS EN HUBSPOT ==================`);
  console.log(`📊 Enviando ${validDeals.length} negocios válidos a HubSpot`);
  console.log(`⏰ Tiempo disponible: ${Math.round(remainingTime / 1000)}s`);
  console.log(`📦 Tamaño de batch: ${BATCH_SIZE}`);
  const totalBatches = Math.ceil(validDeals.length / BATCH_SIZE);

  for (let i = 0; i < validDeals.length; i += BATCH_SIZE) {
    const elapsed = Date.now() - startTime;
    const batchNumber = Math.floor(i / BATCH_SIZE) + 1;

    if (elapsed > remainingTime * 0.9) {
      console.log(`⏰ Timeout alcanzado en batch ${batchNumber}`);
      await savePartialProgress(fileName, totalSubidos, validDeals.length);
      break;
    }

    const batch = validDeals.slice(i, i + BATCH_SIZE);

    try {
      const requestBody = { inputs: batch };
      const res = await fetch(`${HUBSPOT_BASE}/crm/v3/objects/deals/batch/create`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(requestBody),
      });

      if (!res.ok) {
        totalFallidos += batch.length;
        continue;
      }

      const data = await res.json();
      const created = data.results || [];
      totalSubidos += created.length;

      if (created.length < batch.length) {
        totalFallidos += batch.length - created.length;
      }

      if (batchNumber % 5 === 0) {
        await savePartialProgress(fileName, totalSubidos, validDeals.length);
      }
    } catch (err) {
      totalFallidos += batch.length;
    }

    if (batchNumber % 10 === 0 || batchNumber === totalBatches) {
      const percent = ((batchNumber / totalBatches) * 100).toFixed(1);
      console.log(`📊 Progreso: ${batchNumber}/${totalBatches} (${percent}%) - Exitosos: ${totalSubidos}, Fallidos: ${totalFallidos}`);
    }

    await wait(200);
  }

  console.log(`\n🎯 ================== RESUMEN CREACIÓN DE DEALS ==================`);
  console.log(`✅ Exitosos: ${totalSubidos}`);
  console.log(`❌ Fallidos: ${totalFallidos}`);
  console.log(`📈 Tasa de éxito: ${validDeals.length > 0 ? ((totalSubidos / validDeals.length) * 100).toFixed(1) : 0}%`);
  console.log(`===============================================================`);

  return { totalSubidos, totalFallidos };
}

async function generateFinalReport(deals, result, invalidDeals, fileName) {
  const { totalSubidos, totalFallidos } = result;
  const totalOriginal = deals.length;
  const totalSinContacto = invalidDeals.length;

  const now = new Date();
  const report = `📄 REPORTE DE PROCESAMIENTO - ${fileName}

🕒 Fecha: ${now.toLocaleDateString("es-EC")} ${now.toLocaleTimeString("es-EC")}
📊 Total negocios: ${totalOriginal}
✅ Subidos: ${totalSubidos}
❌ Fallidos: ${totalFallidos}
🚫 Sin contacto válido: ${totalSinContacto}
📈 Éxito total: ${((totalSubidos / totalOriginal) * 100).toFixed(1)}%`;

  await saveReportToS3(report, `reporte_${fileName.replace(".csv", "")}.txt`);
}

function wait(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
