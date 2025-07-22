// ./utils/hubspot.js
import fetch from "node-fetch";

export async function sendToHubspot(deals) {
  const apiKey = process.env.HUBSPOT_API_KEY;
  const url = `https://api.hubapi.com/crm/v3/objects/deals/batch/create`;
  const batchSize = 100;

  for (let i = 0; i < deals.length; i += batchSize) {
    const batch = deals.slice(i, i + batchSize);
    
    // Validar que todos los contactos existan antes de enviar el batch
    const validDeals = [];
    for (const deal of batch) {
      const contactExists = await checkContactExists(deal.associations[0].to.id);
      if (contactExists) {
        validDeals.push(deal);
      } else {
        console.warn(`❌ Contacto ${deal.associations[0].to.id} no existe - Negocio "${deal.properties.dealname}" no se subirá`);
      }
    }
    
    if (validDeals.length === 0) {
      console.log(`⚠️ Batch ${i} - ${i + batch.length - 1}: Todos los contactos son inválidos, saltando batch`);
      continue;
    }
    
    const res = await fetch(url, {
      method: "POST",
      headers: { 
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`
      },
      body: JSON.stringify({ inputs: validDeals }),
    });

    if (!res.ok) {
      console.error("❌ Error en batch:", await res.text());
    } else {
      console.log(`✅ Batch enviado: ${i} - ${i + batch.length - 1} (${validDeals.length}/${batch.length} negocios válidos)`);
    }
  }
}

// Función para verificar si existe un contacto en HubSpot
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
    
    return res.ok;
  } catch (error) {
    console.error(`❌ Error verificando contacto ${contactId}:`, error);
    return false;
  }
}