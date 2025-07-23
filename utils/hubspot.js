// ./utils/hubspot.js
import fetch from "node-fetch";

const HUBSPOT_BASE = "https://api.hubapi.com";
const BATCH_SIZE = 100;

export async function sendToHubspot(deals) {
  const apiKey = process.env.HUBSPOT_API_KEY;

  // Agrupar deals por contact_id
  const contactIdToDeals = {};
  for (const deal of deals) {
    const contactId = deal.associations?.[0]?.to?.id;
    if (contactId) {
      if (!contactIdToDeals[contactId]) contactIdToDeals[contactId] = [];
      contactIdToDeals[contactId].push(deal);
    }
  }

  const allContactIds = Object.keys(contactIdToDeals);
  console.log(
    `🔍 Total contactos únicos referenciados: ${allContactIds.length}`
  );

  const contactIdToHubspotId = new Map();

  for (let i = 0; i < allContactIds.length; i += BATCH_SIZE) {
    const batch = allContactIds.slice(i, i + BATCH_SIZE);
    try {
      const res = await fetch(
        `${HUBSPOT_BASE}/crm/v3/objects/contacts/batch/read`,
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${apiKey}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            idProperty: "contact_id",
            inputs: batch.map((id) => ({ id })),
          }),
        }
      );

      if (!res.ok) {
        const error = await res.text();
        console.error(
          `❌ Error al consultar contactos batch ${i}-${i + batch.length - 1}:`,
          error
        );
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

  console.log(`✅ Contactos válidos encontrados: ${contactIdToHubspotId.size}`);

  const validDeals = [];
  const invalids = [];

  for (const contactId of Object.keys(contactIdToDeals)) {
    const negocios = contactIdToDeals[contactId];
    const hubspotId = contactIdToHubspotId.get(contactId);

    if (hubspotId) {
      for (const negocio of negocios) {
        negocio.associations[0].to.id = hubspotId;
        validDeals.push(negocio);
      }
    } else {
      // Contacto no existe
      for (const negocio of negocios) {
        invalids.push({
          dealName: negocio.properties.dealname || "Sin nombre",
          contactId,
        });
      }
    }
  }

  if (invalids.length > 0) {
    console.warn(
      `⚠️ ${invalids.length} negocio(s) omitidos por contactos inexistentes:`
    );
    invalids.forEach(({ dealName, contactId }) => {
      console.warn(
        `   • Negocio: "${dealName}" - Contacto inexistente: ${contactId}`
      );
    });
  }

  // Enviar negocios válidos
  let totalSubidos = 0;

  for (let i = 0; i < validDeals.length; i += BATCH_SIZE) {
    const batch = validDeals.slice(i, i + BATCH_SIZE);

    try {
      const res = await fetch(
        `${HUBSPOT_BASE}/crm/v3/objects/deals/batch/create`,
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${apiKey}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ inputs: batch }),
        }
      );

      if (!res.ok) {
        const error = await res.text();
        console.error(
          `❌ Error al subir batch ${i}-${i + batch.length - 1}:`,
          error
        );
        try {
          const errorData = JSON.parse(error);
          if (errorData.errors) {
            errorData.errors.forEach((err, idx) => {
              console.error(`   Error ${idx}: ${err.message}`);
              if (err.context) {
                console.error(`   Contexto:`, err.context);
              }
            });
          }
        } catch (parseErr) {
          console.error(`   Error crudo:`, error);
        }
      } else {
        console.log(`✅ Subido batch ${i}-${i + batch.length - 1}`);
        totalSubidos += batch.length;
      }
    } catch (err) {
      console.error(
        `❌ Excepción al subir batch ${i}-${i + batch.length - 1}:`,
        err
      );
    }

    await wait(500);
  }

  console.log(
    `📊 Resumen: ${totalSubidos} negocios subidos ✅ | ${invalids.length} omitidos ❌`
  );
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
