// ./utils/hubspot.js
import fetch from "node-fetch";

const HUBSPOT_BASE = "https://api.hubapi.com";
const BATCH_SIZE = 100;

export async function sendToHubspot(deals) {
  const apiKey = process.env.HUBSPOT_API_KEY;

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

  const contactIdToHubspotId = new Map();

  for (let i = 0; i < allContactIds.length; i += BATCH_SIZE) {
    const batch = allContactIds.slice(i, i + BATCH_SIZE);
    try {
      const res = await fetch(`${HUBSPOT_BASE}/crm/v3/objects/contacts/batch/read`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          idProperty: "contact_id",
          inputs: batch.map((id) => ({ id })),
        }),
      });

      if (!res.ok) {
        const error = await res.text();
        console.error(`❌ Error al consultar contactos batch ${i}-${i + batch.length - 1}:`, error);
        continue;
      }

      const data = await res.json();
      for (const contact of data.results || []) {
        const customContactId = contact.properties.contact_id;
        const hubspotId = contact.id;
        contactIdToHubspotId.set(customContactId, hubspotId);
      }
    } catch (err) {
      console.error(`❌ Excepción al consultar batch de contactos:`, err);
    }
    await wait(250);
  }

  console.log(`✅ Contactos válidos encontrados: ${contactIdToHubspotId.size} de ${allContactIds.length}`);

  const validDeals = [];
  const invalidDeals = [];
  const errorSummary = {
    sin_contacto: 0,
    sin_nombre: 0,
    otros: 0,
  };

  for (const contactId of Object.keys(contactIdToDeals)) {
    const negocios = contactIdToDeals[contactId];
    const hubspotId = contactIdToHubspotId.get(contactId);

    if (hubspotId) {
      for (const negocio of negocios) {
        if (!negocio.properties.dealname) {
          errorSummary.sin_nombre++;
          invalidDeals.push({ reason: "Sin nombre", dealName: "(Vacío)", contactId });
        } else {
          negocio.associations[0].to.id = hubspotId;
          validDeals.push(negocio);
        }
      }
    } else {
      errorSummary.sin_contacto += negocios.length;
      for (const negocio of negocios) {
        invalidDeals.push({ reason: "Contacto inexistente", dealName: negocio.properties.dealname || "Sin nombre", contactId });
      }
    }
  }

  let totalSubidos = 0;
  let totalFallidos = 0;

  for (let i = 0; i < validDeals.length; i += BATCH_SIZE) {
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
      } else {
        totalSubidos += batch.length;
      }
    } catch (err) {
      console.error(`❌ Excepción al subir batch ${i}-${i + batch.length - 1}:`, err);
    }
    await wait(500);
  }

  const totalOriginal = deals.length;
  const totalSinContacto = errorSummary.sin_contacto;
  const totalSinNombre = errorSummary.sin_nombre;
  const totalProcesadosConExito = totalSubidos;
  const totalFallidosEnEnvio = totalFallidos;
  const tasaExito = ((totalSubidos / totalOriginal) * 100).toFixed(1);

  return {
    totalOriginal,
    totalProcesadosConExito,
    totalFallidosEnEnvio,
    totalSinContacto,
    totalSinNombre,
    tasaExito,
  };
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
