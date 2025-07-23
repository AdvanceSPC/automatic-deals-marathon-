// ./utils/hubspot.js
import fetch from "node-fetch";

const HUBSPOT_BASE = "https://api.hubapi.com";
const BATCH_SIZE = 100;

export async function sendToHubspot(deals) {
  const apiKey = process.env.HUBSPOT_API_KEY;

  // Paso 1: Obtener todos los contact_ids únicos del archivo
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

  // Paso 2: Verificar cuáles contactos existen por contact_id (idProperty)
  const existingContactIds = new Set();

  for (let i = 0; i < allContactIds.length; i += BATCH_SIZE) {
    const batch = allContactIds.slice(i, i + BATCH_SIZE);
    try {
      const res = await fetch(`${HUBSPOT_BASE}/crm/v3/objects/contacts/batch/read`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          idProperty: "contact_id",
          inputs: batch.map(id => ({ id }))
        }),
      });

      if (!res.ok) {
        const error = await res.text();
        console.error(`❌ Error al consultar contactos batch ${i}-${i + batch.length - 1}:`, error);
        continue;
      }

      const data = await res.json();
      for (const contact of data.results || []) {
        existingContactIds.add(contact.properties.contact_id);
      }

    } catch (err) {
      console.error(`❌ Excepción al consultar batch de contactos:`, err);
    }

    await wait(250);
  }

  console.log(`✅ Contactos válidos encontrados: ${existingContactIds.size}`);

  // Paso 3: Filtrar negocios que tienen contactos válidos
  const validDeals = [];
  const invalids = [];

  for (const contactId of Object.keys(contactIdToDeals)) {
    const negocios = contactIdToDeals[contactId];
    if (existingContactIds.has(contactId)) {
      validDeals.push(...negocios);
    } else {
      for (const negocio of negocios) {
        invalids.push({ dealName: negocio.properties.dealname || "Sin nombre", contactId });
      }
    }
  }

  if (invalids.length > 0) {
    console.warn(`⚠️ ${invalids.length} negocio(s) omitidos por contactos inexistentes:`);
    invalids.forEach(({ dealName, contactId }) => {
      console.warn(`   • Negocio: "${dealName}" - Contacto inexistente: ${contactId}`);
    });
  }

  // Paso 4: Enviar negocios válidos a HubSpot por lotes
  let totalSubidos = 0;

  for (let i = 0; i < validDeals.length; i += BATCH_SIZE) {
    const batch = validDeals.slice(i, i + BATCH_SIZE);

    try {
      const res = await fetch(`${HUBSPOT_BASE}/crm/v3/objects/deals/batch/create`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ inputs: batch }),
      });

      if (!res.ok) {
        const error = await res.text();
        console.error(`❌ Error al subir batch ${i}-${i + batch.length - 1}:`, error);
      } else {
        console.log(`✅ Subido batch ${i}-${i + batch.length - 1}`);
        totalSubidos += batch.length;
      }
    } catch (err) {
      console.error(`❌ Excepción al subir batch ${i}-${i + batch.length - 1}:`, err);
    }

    await wait(500);
  }

  console.log(`📊 Resumen: ${totalSubidos} negocios subidos ✅ | ${invalids.length} omitidos ❌`);
}

function wait(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
