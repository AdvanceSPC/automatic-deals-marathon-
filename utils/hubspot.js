// ./utils/hubspot.js
import fetch from "node-fetch";

export async function sendToHubspot(deals) {
  const apiKey = process.env.HUBSPOT_API_KEY;
  const url = `https://api.hubapi.com/crm/v3/objects/deals/batch/create`;
  const batchSize = 100;

  let totalProcessed = 0;
  let totalSkipped = 0;

  for (let i = 0; i < deals.length; i += batchSize) {
    const batch = deals.slice(i, i + batchSize);
    const batchIndex = Math.floor(i / batchSize) + 1;
    console.log(`🔍 Validando contactos del batch ${batchIndex}...`);

    const validationResults = await Promise.all(
      batch.map(async (deal) => {
        const contactId = deal.associations[0].to.id;
        const contactExists = await checkContactExists(contactId);
        return {
          deal,
          contactExists,
          contactId,
          dealName: deal.properties.dealname || 'Sin nombre',
        };
      })
    );

    const validDeals = validationResults.filter(r => r.contactExists).map(r => r.deal);
    const invalidContactIds = validationResults.filter(r => !r.contactExists);

    if (invalidContactIds.length > 0) {
      console.warn(`❌ ${invalidContactIds.length} negocio(s) NO se subirán por contactos inexistentes:`);
      invalidContactIds.forEach(item => {
        console.warn(`   • Negocio: "${item.dealName}" - Contacto inexistente: ${item.contactId}`);
      });
      totalSkipped += invalidContactIds.length;
    }

    if (validDeals.length === 0) {
      console.log(`⚠️ Batch ${batchIndex}: Todos los contactos son inválidos, saltando batch completo`);
      continue;
    }

    try {
      const res = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${apiKey}`
        },
        body: JSON.stringify({ inputs: validDeals }),
      });

      if (!res.ok) {
        console.error(`❌ Error en batch ${batchIndex}:`, await res.text());
      } else {
        totalProcessed += validDeals.length;
        console.log(`✅ Batch ${batchIndex} enviado exitosamente: ${validDeals.length}/${batch.length} negocios`);
      }
    } catch (err) {
      console.error(`❌ Excepción en el envío del batch ${batchIndex}:`, err);
    }
  }

  console.log(`📊 Resumen final: ${totalProcessed} negocios subidos ✅ | ${totalSkipped} negocios omitidos ❌`);
}

// Verificar si existe un contacto en HubSpot
async function checkContactExists(contactId) {
  const apiKey = process.env.HUBSPOT_API_KEY;
  const url = `https://api.hubapi.com/crm/v3/objects/contacts/${contactId}`;

  try {
    const res = await fetch(url, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${apiKey}`
      }
    });

    if (res.status === 200) return true;
    if (res.status === 404) return false;

    const errorText = await res.text();
    console.error(`❌ Error inesperado al verificar contacto ${contactId}: ${res.status} - ${errorText}`);
    return false;
  } catch (error) {
    console.error(`❌ Excepción al verificar contacto ${contactId}:`, error);
    return false;
  }
}
