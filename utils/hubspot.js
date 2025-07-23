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
  console.log(`🔍 Total contactos únicos referenciados: ${allContactIds.length}`);

  const contactIdToHubspotId = new Map();

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

  // filtrar y actualizar deals con IDs de HubSpot
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
      // Contacto no existe
      for (const negocio of negocios) {
        invalidDeals.push({ 
          dealName: negocio.properties.dealname || "Sin nombre", 
          contactId 
        });
      }
    }
  }



  if (invalidDeals.length > 0) {
    console.log(`⚠️ NEGOCIOS QUE NO SE SUBIRÁN (${invalidDeals.length}):`);
    const samplesToShow = Math.min(invalidDeals.length, 10);
    for (let i = 0; i < samplesToShow; i++) {
      const { dealName, contactId } = invalidDeals[i];
      console.warn(`   • "${dealName}" - Contacto inexistente: ${contactId}`);
    }
    if (invalidDeals.length > 10) {
      console.warn(`   ... y ${invalidDeals.length - 10} más`);
    }
    console.log(); 
  }

  // enviar negocios válidos
  let totalSubidos = 0;
  let totalFallidos = 0;

  if (validDeals.length === 0) {
    console.log("⚠️ No hay negocios válidos para subir.");
  } else {
    console.log(`🚀 Enviando ${validDeals.length} negocios válidos a HubSpot...`);
  }

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
        totalFallidos += batch.length;
        
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
      console.error(`❌ Excepción al subir batch ${i}-${i + batch.length - 1}:`, err);
    }

    await wait(500);
  }

  const totalOriginal = deals.length;
  const totalProcesadosConExito = totalSubidos;
  const totalFallidosEnEnvio = totalFallidos;
  const totalSinContacto = invalidDeals.length;
  
  console.log(`\n🎯 ================== RESUMEN FINAL ==================`);
  console.log(`📄 Total negocios en archivo: ${totalOriginal}`);
  console.log(`✅ Subidos exitosamente: ${totalProcesadosConExito}`);
  console.log(`❌ Fallidos en envío: ${totalFallidosEnEnvio}`);
  console.log(`🚫 Sin contacto válido: ${totalSinContacto}`);
  console.log(`📊 Tasa de éxito: ${((totalProcesadosConExito / totalOriginal) * 100).toFixed(1)}%`);
  console.log(`==================================================\n`);
}

function wait(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
